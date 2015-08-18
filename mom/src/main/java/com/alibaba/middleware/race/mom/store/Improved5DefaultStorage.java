package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.Parameter;
import com.alibaba.middleware.race.mom.bean.MessageId;
import com.alibaba.middleware.race.mom.util.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by slade on 2015/8/8.
 */
public class Improved5DefaultStorage implements Storage {
    private ConcurrentHashMap<MessageId, OffsetState/*offset and state in headerFile*/> headerLookupTable;
    private BlockingQueue<StorageUnit> insertionTaskQueue;
    private ConcurrentHashMap<MessageId, StorageCallback<Boolean>> insertionStateTable;

    private BlockingQueue<MessageId> markSuccessQueue;

    private volatile boolean stop = true;
    private AsynchronousFileChannel messageChannel;

    public void start() {
        if (!stop) {
            throw new IllegalStateException("already started");
        }
        stop = false;

        Path storeDir = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store");

        if (!Files.exists(storeDir) && !Files.isDirectory(storeDir)) {
            try {
                Files.createDirectory(storeDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            Path bodyFile = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store", "message.data");
            messageChannel = AsynchronousFileChannel.open(bodyFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC);
        } catch (IOException e) {
            e.printStackTrace();
        }

        headerLookupTable = new ConcurrentHashMap<MessageId, OffsetState>();
        insertionTaskQueue = new LinkedBlockingQueue<StorageUnit>();
        insertionStateTable = new ConcurrentHashMap<MessageId, StorageCallback<Boolean>>();

        new InsertTaskProducer().start();

        markSuccessQueue = new PriorityBlockingQueue<MessageId>(11, new Comparator<MessageId>() {
            @Override
            public int compare(MessageId o1, MessageId o2) {
                OffsetState state1 = headerLookupTable.get(o1);
                OffsetState state2 = headerLookupTable.get(o2);
                int offset1 = state1 != null ? (int) state1.offset : 0;
                int offset2 = state2 != null ? (int) state2.offset : 0;
                return offset1 - offset2;
            }
        });
        new MarkSuccessWorker().start();
    }

    public void stop() {
        if (stop) {
            throw new IllegalStateException("already stopped");
        }
        stop = true;

        if (messageChannel != null) {
            try {
                messageChannel.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Override
    public void insert(StorageUnit storageUnit, StorageCallback<Boolean> callback) {
        try {
            insertionStateTable.put(storageUnit.msgId(), callback);
            insertionTaskQueue.put(storageUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void markSuccess(MessageId msgId) {
        try {
            markSuccessQueue.put(msgId);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void markFail(MessageId msgId) {
        OffsetState offsetState = headerLookupTable.get(msgId);
        if (offsetState != null) {
            offsetState.state = MessageState.FAIL;
        }
    }


    @Override
    public void failList(StorageCallback<List<StorageUnit>> callback) {
        LinkedList<StorageUnit> failList = new LinkedList<StorageUnit>();
        //TODO failList signal two water marks:try get & get
        ByteBuffer lastMsg = null;
        ByteBuffer thisMsg;
        OffsetState state;
        Iterator<MessageId> iterator = headerLookupTable.keySet().iterator();

        while (iterator.hasNext() && failList.size() < Parameter.RESEND_NUM) {
            try {
                state = headerLookupTable.get(iterator.next());
                if (state.state == MessageState.RESEND) {
                    continue;
                }
                thisMsg = ByteBuffer.allocate(state.length);
                messageChannel.read(thisMsg, state.offset);
                failList.add(new StorageUnit().msg(lastMsg));
                state.state = MessageState.RESEND;
                lastMsg = thisMsg;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        callback.complete(failList);
    }


    private class InsertTaskParameter {
        ByteBuffer msgBlock;
        ArrayList<MessageId> msgIds;
        ArrayList<OffsetState> offsetStates;

        InsertTaskParameter(int sizeTo4K) {
            this.msgBlock = ByteBuffer.allocate((sizeTo4K >> 12) + 1 << 12);
            this.msgIds = new ArrayList<MessageId>();
            this.offsetStates = new ArrayList<OffsetState>();
        }
    }

    private class InsertTaskProducer extends Thread {
        private long offset, writePosition;
        private InsertTaskParameter currentPara;
        private LinkedList<InsertTaskParameter> finishPara;

        public InsertTaskProducer() {
            super("insertion Task Producer");
            try {
                offset = messageChannel.size();
                writePosition = offset;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            finishPara = new LinkedList<InsertTaskParameter>();
        }

        @Override
        public void run() {
            long lastFlush = System.currentTimeMillis();
            while (!stop) {
                try {
                    LinkedList<StorageUnit> list = new LinkedList<StorageUnit>();
                    StorageUnit unit = insertionTaskQueue.poll(5, TimeUnit.MILLISECONDS);
                    if (unit != null) {
                        list.add(unit);
                        insertionTaskQueue.drainTo(list);

                        for (StorageUnit u : list) {
                            int msgSize = u.msg().capacity() + 1/* align 4K ? */;
                            if (currentPara == null) {
                                currentPara = new InsertTaskParameter(msgSize);
                            } else {
                                int remaining = currentPara.msgBlock.remaining();
                                if (remaining < msgSize) {
                                    if (remaining > 0) {
                                        align4K();
                                    }
                                    finishPara.add(currentPara);
                                    currentPara = new InsertTaskParameter(msgSize);
                                }
                            }
                            currentPara.msgIds.add(u.msgId());
                            currentPara.msgBlock.put((byte) 0); /* not align to 4K */
                            offset++;
                            currentPara.msgBlock.put(u.msg());
                            int length = u.msg().capacity();
                            currentPara.offsetStates.add(new OffsetState(offset, length, MessageState.RESEND));
                            offset += length;
                        }
                    }

                    long currentMills = System.currentTimeMillis();
                    if (currentMills - lastFlush > Parameter.FLUSH_DISK_TIME_THRESHOLD_MILLIS) {
                        lastFlush = currentMills;
                        if (currentPara != null) {
                            align4K();
                            finishPara.add(currentPara);
                        }
                        currentPara = null;

                        for (InsertTaskParameter parameter : finishPara) {
//                        final long startTime = System.currentTimeMillis();
                            parameter.msgBlock.position(0);
                            messageChannel.write(parameter.msgBlock, writePosition, parameter, new CompletionHandler<Integer, InsertTaskParameter>() {
                                @Override
                                public void completed(Integer result, InsertTaskParameter attachment) {
//                                System.out.println(System.currentTimeMillis() - startTime);
                                    int i = 0;
                                    for (MessageId msgId : attachment.msgIds) {
                                        headerLookupTable.put(msgId, attachment.offsetStates.get(i++));
                                        insertionStateTable.get(msgId).complete(true);
                                    }
                                }

                                @Override
                                public void failed(Throwable exc, InsertTaskParameter attachment) {
                                    for (MessageId msgId : attachment.msgIds) {
                                        insertionStateTable.get(msgId).complete(false);
                                    }
                                }
                            });
                            writePosition += parameter.msgBlock.capacity();
                        }
                        finishPara.clear();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void align4K() {
            currentPara.msgBlock.put((byte) 1); /* align to 4K */
            offset = (offset >> 12) + 1 << 12;
        }
    }

    private class MarkSuccessWorker extends Thread {
        public MarkSuccessWorker() {
            super("markSuccess worker");
        }

        @Override
        public void run() {
            while (!stop) {
                LinkedList<MessageId> list = new LinkedList<MessageId>();
                try {
                    list.add(markSuccessQueue.take());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                markSuccessQueue.drainTo(list);

                try {
                    for (final MessageId msgId : list) {
                        OffsetState offsetState = headerLookupTable.get(msgId);
                        if (offsetState != null) {
                            messageChannel.write(ByteBuffer.allocate(4).putInt(MessageState.SUCCESS.ordinal()), offsetState.offset + StorageUnit.STATE_OFFSET);
                        } else {
                            Logger.error("[markSuccess unknown message id] %s", msgId);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    class OffsetState {
        long offset;
        int length;
        MessageState state;

        public OffsetState(long offset, int length, MessageState state) {
            this.offset = offset;
            this.length = length;
            this.state = state;
        }
    }
}
