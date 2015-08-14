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
public class ImprovedImproved3DefaultStorage implements Storage {
    private ConcurrentHashMap<MessageId, OffsetState/*offset and state in headerFile*/> headerLookupTable;
    private BlockingQueue<StorageUnit> insertionTaskQueue;
    private ConcurrentHashMap<MessageId, StorageCallback<Boolean>> insertionStateTable;

    private BlockingQueue<MessageId> markSuccessQueue;


    private volatile boolean stop = true;
    private AsynchronousFileChannel bodyChannel;
    private AsynchronousFileChannel headerChannel;
    private BlockingQueue<IOInsertParameter> producer2ioParameterHolder;
    private BlockingQueue<IOInsertParameter> io2PostExecutorParameterHolder;
    private volatile boolean ioRequest;

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
        producer2ioParameterHolder = new ArrayBlockingQueue<IOInsertParameter>(1);
        io2PostExecutorParameterHolder = new LinkedBlockingQueue<IOInsertParameter>();

        new InsertTaskProducer().start();
        new InsertIOWorker().start();

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

    private void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }

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

    private class IOInsertParameter {
        StorageUnit unit;
        ArrayList<MessageId> msgIds;
        ArrayList<OffsetState> offsetStates;

        public IOInsertParameter unit(StorageUnit unit) {
            this.unit = unit;
            return this;
        }

        public IOInsertParameter msgIds(ArrayList<MessageId> msgIds) {
            this.msgIds = msgIds;
            return this;
        }

        public IOInsertParameter offsetStates(ArrayList<OffsetState> offsetStates) {
            this.offsetStates = offsetStates;
            return this;
        }

        @Override
        public String toString() {
            return "IOInsertParameter{" +
                    "batch size=" + offsetStates.size() +
                    '}';
        }
    }

    private class InsertTaskProducer extends Thread {
        IOInsertParameter parameter;
        long bodyOffset, headerOffset;

        public InsertTaskProducer() {
            super("Insert Task Producer");
            initUnit();
            try {
                this.bodyOffset = bodyChannel.size();
                this.headerOffset = headerChannel.size();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    LinkedList<StorageUnit> list = new LinkedList<StorageUnit>();
                    list.add(insertionTaskQueue.take());
                    insertionTaskQueue.drainTo(list);
                    for (StorageUnit u : list) {
                        Logger.info("[get insert task] %s", u);
                        parameter.unit.body().put(u.body());
                        byte[] msgId = new byte[MessageId.LENGTH];
                        u.header().get(msgId);
                        u.header().limit(StorageUnit.HEADER_LENGTH);
                        u.header().position(0);
                        u.header().putLong(20, bodyOffset);
                        parameter.msgIds.add(new MessageId(msgId));
                        parameter.unit.header().put(u.header());
                        int bodyLength = u.body().capacity();
                        parameter.offsetStates.add(new OffsetState(headerOffset, bodyLength, bodyOffset, MessageState.RESEND));
                        headerOffset += StorageUnit.HEADER_LENGTH;
                        bodyOffset += bodyLength;
                    }
                    if (ioRequest) {
                        try {
                            ioRequest = false;
                            parameter.unit.header().flip();
                            parameter.unit.body().flip();
                            producer2ioParameterHolder.put(parameter);
                            Logger.info("[generate io task] %s", parameter);
                            initUnit();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        void initUnit() {
            this.parameter = new IOInsertParameter()
                    .unit(new StorageUnit().body(ByteBuffer.allocate(16 * 1024 * 1024)).header(ByteBuffer.allocate(8 * 1024 * 1024)))
                    .msgIds(new ArrayList<MessageId>())
                    .offsetStates(new ArrayList<OffsetState>());
        }
    }

    private class InsertIOWorker extends Thread {

        public InsertIOWorker() {
            super("Insert Worker");
        }

        @Override
        public void run() {
            ioRequest = true;
            try {
                while (!stop) {
                    IOInsertParameter parameter = producer2ioParameterHolder.take();
                    Logger.info("[get io task] %s", parameter);
                    headerChannel.write(parameter.unit.header(), headerChannel.size());
                    bodyChannel.write(parameter.unit.body(), bodyChannel.size(), parameter, new CompletionHandler<Integer, IOInsertParameter>() {
                        @Override
                        public void completed(Integer result, IOInsertParameter parameter) {
                            int i = 0;
                            for (MessageId msgId : parameter.msgIds) {
                                headerLookupTable.put(msgId, parameter.offsetStates.get(i++));
                                insertionStateTable.get(msgId).complete(true);
                            }
                            ioRequest = true;
                        }

                        @Override
                        public void failed(Throwable exc, IOInsertParameter parameter) {
                            for (MessageId msgId : parameter.msgIds) {
                                insertionStateTable.get(msgId).complete(false);
                            }
                            ioRequest = true;
                        }
                    });
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
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
                } catch (Exception e) {
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
