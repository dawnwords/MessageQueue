package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.Parameter;
import com.alibaba.middleware.race.mom.bean.MessageId;
import com.alibaba.middleware.race.mom.util.Logger;

import java.io.Closeable;
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
public class ImprovedDefaultStorage implements Storage {
    private ConcurrentHashMap<MessageId, OffsetState/*offset and state in headerFile*/> headerLookupTable;
    private BlockingQueue<StorageUnit> insertionTaskQueue;
    private ConcurrentHashMap<MessageId, StorageCallback<Boolean>> insertionStateTable;

    private BlockingQueue<MessageId> markSuccessQueue;

    private volatile boolean stop = true;
    private AsynchronousFileChannel bodyChannel;
    private AsynchronousFileChannel headerChannel;

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

        Path headerFile = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store", "header.msg");
        Path bodyFile = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store", "body.msg");

        try {
            bodyChannel = AsynchronousFileChannel.open(bodyFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC);
            headerChannel = AsynchronousFileChannel.open(headerFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // should after bodyChannel and headerChannel open.
//        indexRebuild();

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

        close(headerChannel);
        close(bodyChannel);

    }

    private void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
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
        ByteBuffer lastBody = null;
        ByteBuffer thisBody;
        OffsetState state;
        Future<Integer> future = null;
        MessageId lastId = null;
        MessageId thisId;
        Iterator<MessageId> iterator = headerLookupTable.keySet().iterator();
        while (iterator.hasNext()) {
            lastId = iterator.next();
            state = headerLookupTable.get(lastId);
            if (state.state == MessageState.FAIL) {
                lastBody = ByteBuffer.allocate(state.bodyLength);
                future = bodyChannel.read(lastBody, state.bodyOffset);
                break;
            }
        }
        while (iterator.hasNext() && failList.size() < Parameter.RESEND_NUM) {
            try {
                thisId = iterator.next();
                state = headerLookupTable.get(thisId);
                if (state.state == MessageState.RESEND) {
                    continue;
                }
                thisBody = ByteBuffer.allocate(state.bodyLength);
                future.get();
                future = bodyChannel.read(thisBody, state.bodyOffset);
                ByteBuffer header = ByteBuffer.allocate(StorageUnit.HEADER_LENGTH);
                header.put(lastId.id());
                header.flip();
                failList.add(new StorageUnit().header(header).body(lastBody));
                state.state = MessageState.RESEND;
                lastId = thisId;
                lastBody = thisBody;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        callback.complete(failList);
    }

    private void indexRebuild() {
        ByteBuffer buf = ByteBuffer.allocate(Parameter.INDEX_LOAD_BUFF_SIZE);
        long startPos = 0l;
        Integer bytesRead = 0;

        do {
            try {
                bytesRead = headerChannel.read(buf, startPos).get();
                buildIndexEntry(startPos, bytesRead, buf);
                startPos += bytesRead;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } while (bytesRead > 0);
    }

    private void buildIndexEntry(long startPos, int bytesRead, ByteBuffer buf) {
        for (int curPos = 0; curPos < bytesRead; startPos += StorageUnit.HEADER_LENGTH) {
            if (MessageState.values()[buf.getInt(curPos + 28)] == MessageState.FAIL) {
                byte[] messageId = new byte[MessageId.LENGTH];
                buf.get(messageId);
                buf.position(MessageId.LENGTH);
                headerLookupTable.put(new MessageId(messageId), new OffsetState(startPos, buf.getInt(), buf.getLong(), MessageState.values()[buf.getInt()]));
            }
        }

    }

    private class InsertWorker extends Thread {

        @Override
        public void run() {
            while (!stop) {
                try {
                    LinkedList<StorageUnit> list = new LinkedList<StorageUnit>();
                    list.add(insertionTaskQueue.take());
                    insertionTaskQueue.drainTo(list);

                    int headerSize = 0;
                    int bodySize = 0;
                    for (StorageUnit u : list) {
                        headerSize += StorageUnit.HEADER_LENGTH;
                        bodySize += u.body().capacity();
                    }
                    ByteBuffer headerBlock = ByteBuffer.allocate(headerSize);
                    ByteBuffer bodyBlock = ByteBuffer.allocate(bodySize);

                    ArrayList<OffsetState> offsetStates = new ArrayList<OffsetState>();
                    long bodyTail = bodyChannel.size();
                    long headerTail = headerChannel.size();
                    for (StorageUnit u : list) {
                        bodyBlock.put(u.body());
                        u.header().putLong(20, bodyTail);
                        headerBlock.put(u.header());
                        int bodyLength = u.body().capacity();
                        offsetStates.add(new OffsetState(headerTail, bodyLength, bodyTail, MessageState.RESEND));
                        headerTail += StorageUnit.HEADER_LENGTH;
                        bodyTail += bodyLength;
                    }
                    headerBlock.flip();
                    bodyBlock.flip();
                    Future<Integer> bodyFuture = bodyChannel.write(bodyBlock, bodyChannel.size());
                    Future<Integer> headerFuture = headerChannel.write(headerBlock, headerChannel.size());
                    headerFuture.get();
                    bodyFuture.get();
                    int i = 0;
                    for (StorageUnit unit : list) {
                        unit.header().position(0);
                        unit.header().limit(StorageUnit.HEADER_LENGTH);
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

                for (final MessageId msgId : list) {
                    OffsetState offsetState = headerLookupTable.get(msgId);
                    if (offsetState != null) {
                        headerChannel.write(ByteBuffer.allocate(4).putInt(MessageState.SUCCESS.ordinal()), offsetState.offset + 28, null, new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer result, Void attachment) {
                                headerLookupTable.remove(msgId);
                            }

                            @Override
                            public void failed(Throwable exc, Void attachment) {
                            }
                        });
                    } else {
                        Logger.error("[markSuccess unknown message id] %s", msgId);
                    }

                }
            }
        }
    }

    class OffsetState {
        long offset;
        long bodyOffset;
        int bodyLength;
        MessageState state;

        public OffsetState(long offset, int bodyLength, long bodyOffset, MessageState state) {
            this.offset = offset;
            this.bodyOffset = bodyOffset;
            this.bodyLength = bodyLength;
            this.state = state;
        }
    }
}