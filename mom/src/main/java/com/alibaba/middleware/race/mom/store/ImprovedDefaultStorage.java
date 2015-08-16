package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.Parameter;
import com.alibaba.middleware.race.mom.bean.MessageId;
import com.alibaba.middleware.race.mom.util.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by slade on 2015/8/8.
 */
public class ImprovedDefaultStorage implements Storage {
    private ConcurrentHashMap<MessageId, OffsetState/*offset and state in headerFile*/> headerLookupTable;
    private BlockingQueue<StorageUnit> insertionTaskQueue;
    private ConcurrentHashMap<MessageId, StorageCallback<Boolean>> insertionStateTable;

    private BlockingQueue<MessageId> markSuccessQueue;

    private volatile boolean stop = true;
    private FileChannel messageChannel;

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
            messageChannel = FileChannel.open(bodyFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC);
        } catch (IOException e) {
            e.printStackTrace();
        }

        headerLookupTable = new ConcurrentHashMap<MessageId, OffsetState>();
        insertionTaskQueue = new LinkedBlockingQueue<StorageUnit>();
        insertionStateTable = new ConcurrentHashMap<MessageId, StorageCallback<Boolean>>();

        new InsertWorker().start();

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
        ByteBuffer message;
        OffsetState state;
        Iterator<MessageId> iterator = headerLookupTable.keySet().iterator();

        while (iterator.hasNext() && failList.size() < Parameter.RESEND_NUM) {
            try {
                state = headerLookupTable.get(iterator.next());
                if (state.state == MessageState.RESEND) {
                    continue;
                }
                message = ByteBuffer.allocate(state.length);
                messageChannel.read(message, state.offset);
                failList.add(new StorageUnit().msg(message));
                state.state = MessageState.RESEND;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        callback.complete(failList);
    }

    private class InsertWorker extends Thread {

        private long offset;

        public InsertWorker() {
            super("insertion worker");
            try {
                offset = messageChannel.size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    LinkedList<StorageUnit> list = new LinkedList<StorageUnit>();
                    list.add(insertionTaskQueue.take());
                    insertionTaskQueue.drainTo(list);

                    int msgSize = 0;
                    for (StorageUnit u : list) {
                        msgSize += u.msg().capacity();
                    }
                    ByteBuffer msgBlock = ByteBuffer.allocate(msgSize);

                    ArrayList<OffsetState> offsetStates = new ArrayList<OffsetState>();
                    for (StorageUnit u : list) {
                        msgBlock.put(u.msg());
                        int length = u.msg().capacity();
                        offsetStates.add(new OffsetState(offset, length, MessageState.RESEND));
                        offset += length;
                    }
                    msgBlock.flip();
                    messageChannel.write(msgBlock, messageChannel.size());
                    int i = 0;
                    for (StorageUnit unit : list) {
                        MessageId msgId = unit.msgId();
                        headerLookupTable.put(msgId, offsetStates.get(i++));
                        insertionStateTable.get(msgId).complete(true);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
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
