package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.Parameter;
import com.alibaba.middleware.race.mom.bean.MessageId;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by slade on 2015/8/13.
 */
public class MMapStorage implements Storage {

    private ConcurrentHashMap<MessageId, OffsetState/*offset and state in headerFile*/> headerLookupTable;
    private BlockingQueue<StorageUnit> insertionTaskQueue;
    private ConcurrentHashMap<MessageId, StorageCallback<Boolean>> insertionStateTable;

    private BlockingQueue<MessageId> markSuccessQueue;

    private volatile boolean stop = true;

    private FileChannel bodyChannel, headerChannel;

    @Override
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

        String base = System.getProperty("user.home") + File.separator + "store" + File.separator;

        try {
            this.bodyChannel = new RandomAccessFile(base + "body.msg", "rws").getChannel();
            this.headerChannel = new RandomAccessFile(base + "header.msg", "rws").getChannel();
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    @Override
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
        MessageId lastId = null;
        MessageId thisId;
        Iterator<MessageId> iterator = headerLookupTable.keySet().iterator();
        try {
            while (iterator.hasNext()) {
                lastId = iterator.next();
                state = headerLookupTable.get(lastId);
                if (state.state == MessageState.FAIL) {
                    lastBody = ByteBuffer.allocate(state.bodyLength);
                    bodyChannel.read(lastBody, state.bodyOffset);
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while (iterator.hasNext() && failList.size() < Parameter.RESEND_NUM) {
            try {
                thisId = iterator.next();
                state = headerLookupTable.get(thisId);
                if (state.state == MessageState.RESEND) {
                    continue;
                }
                thisBody = ByteBuffer.allocate(state.bodyLength);
                bodyChannel.read(thisBody, state.bodyOffset);
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

    private class InsertWorker extends Thread {

        private long bodyTail;
        private long headerTail;

        public InsertWorker() {
            super("insertion worker");
            try {
                bodyTail = bodyChannel.size();
                headerTail = headerChannel.size();
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

                    int headerSize = 0;
                    int bodySize = 0;
                    for (StorageUnit u : list) {
                        headerSize += StorageUnit.HEADER_LENGTH;
                        bodySize += u.body().capacity();
                    }
                    ByteBuffer headerBlock = ByteBuffer.allocate(headerSize);
                    ByteBuffer bodyBlock = ByteBuffer.allocate(bodySize);

                    ArrayList<OffsetState> offsetStates = new ArrayList<OffsetState>();

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

                    MappedByteBuffer headerBuffer = headerChannel.map(FileChannel.MapMode.READ_WRITE, headerTail - headerSize, headerSize);
                    MappedByteBuffer bodyBuffer = bodyChannel.map(FileChannel.MapMode.READ_WRITE, bodyTail - bodySize, bodySize);
                    headerBuffer.put(headerBlock);
                    bodyBuffer.put(bodyBlock);
                    headerBuffer.force();
                    bodyBuffer.force();

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
                            headerChannel.write(ByteBuffer.allocate(4).putInt(MessageState.SUCCESS.ordinal()), offsetState.offset + 28);
                            headerLookupTable.remove(msgId);
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
