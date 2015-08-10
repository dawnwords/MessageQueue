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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by slade on 2015/8/8.
 */
public class DefaultStorage implements Storage {
    private ConcurrentHashMap<MessageId, OffsetState/*offset and state in headerFile*/> headerLookupTable;
    private BlockingQueue<StorageUnit> insertionTaskQueue;
    private ConcurrentHashMap<MessageId, BlockingQueue<Boolean>> insertionStateTable;

    private BlockingQueue<MessageId> markSuccessQueue;
    private ConcurrentHashMap<MessageId, BlockingQueue<Boolean>> markSuccessStateTable;


    private volatile boolean stop = true;
    private AsynchronousFileChannel bodyChannel;
    private AsynchronousFileChannel headerChannel;

    public void start() {
        if (!stop) {
            throw new IllegalStateException("already started");
        }
        stop = false;

        Path storeDir = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store");

        if(!Files.exists(storeDir) && !Files.isDirectory(storeDir)){
            try {
                Files.createDirectory(storeDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Path headerFile = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store", "header.msg");
        Path bodyFile = FileSystems.getDefault().getPath(System.getProperty("user.home"), "store", "body.msg");

        try {
            bodyChannel = AsynchronousFileChannel.open(bodyFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            headerChannel = AsynchronousFileChannel.open(headerFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        headerLookupTable = new ConcurrentHashMap<MessageId, OffsetState>();
        insertionTaskQueue = new LinkedBlockingQueue<StorageUnit>();
        insertionStateTable = new ConcurrentHashMap<MessageId, BlockingQueue<Boolean>>();

        new InsertWorker().start();

        markSuccessQueue = new LinkedBlockingQueue<MessageId>();
        markSuccessStateTable = new ConcurrentHashMap<MessageId, BlockingQueue<Boolean>>();
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

    private void put(BlockingQueue<Boolean> queue, boolean result) {
        try {
            queue.put(result);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
    public boolean insert(StorageUnit storageUnit) {
        try {
            ArrayBlockingQueue<Boolean> resultHolder = new ArrayBlockingQueue<Boolean>(1);
            insertionStateTable.put(storageUnit.msgId(), resultHolder);
            insertionTaskQueue.put(storageUnit);
            return resultHolder.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean markSuccess(MessageId msgId) {
        try {
            ArrayBlockingQueue<Boolean> resultHolder = new ArrayBlockingQueue<Boolean>(1);
            markSuccessStateTable.put(msgId, resultHolder);
            markSuccessQueue.put(msgId);
            return resultHolder.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean markFail(MessageId msgId) {
        OffsetState offsetState = headerLookupTable.get(msgId);
        if (offsetState != null) {
            offsetState.state = MessageState.FAIL;
            return true;
        }
        return false;
    }


    @Override
    public List<StorageUnit> failList() {
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

        return failList;
    }

    private class InsertWorker extends Thread {

        @Override
        public void run() {
            while (!stop) {
                LinkedList<StorageUnit> list = new LinkedList<StorageUnit>();
                insertionTaskQueue.drainTo(list);

                for (final StorageUnit unit : list) {
                    final MessageId msgId = unit.msgId();
                    final BlockingQueue<Boolean> resultQueue = insertionStateTable.get(msgId);
                    try {
                        final long bodyOffsetBeforeInsert = bodyChannel.size();
                        bodyChannel.write(unit.body(), bodyOffsetBeforeInsert, resultQueue, new CompletionHandler<Integer, BlockingQueue<Boolean>>() {

                            @Override
                            public void completed(Integer result, BlockingQueue<Boolean> attachment) {
                                ByteBuffer headerByteBuffer = unit.header().putLong(20, bodyOffsetBeforeInsert);
                                try {
                                    final long headerOffsetBeforeInsert = headerChannel.size();
                                    //TODO should probably add lock
                                    headerChannel.write(headerByteBuffer, headerOffsetBeforeInsert, resultQueue, new CompletionHandler<Integer, BlockingQueue<Boolean>>() {
                                        @Override
                                        public void completed(Integer result, BlockingQueue<Boolean> attachment) {
                                            headerLookupTable.put(msgId, new OffsetState(headerOffsetBeforeInsert, bodyOffsetBeforeInsert, unit.body().capacity(), MessageState.FAIL));
                                            put(resultQueue, true);
                                        }

                                        @Override
                                        public void failed(Throwable exc, BlockingQueue<Boolean> attachment) {
                                            put(resultQueue, false);
                                        }
                                    });
                                } catch (IOException e) {
                                    put(resultQueue, false);
                                } catch (Exception e) {
                                    new RuntimeException(e);
                                }
                            }

                            @Override
                            public void failed(Throwable exc, BlockingQueue<Boolean> attachment) {
                                put(resultQueue, false);
                            }
                        });
                    } catch (IOException e) {
                        put(resultQueue, false);
                    }
                }
            }
        }
    }

    private class MarkSuccessWorker extends Thread {

        @Override
        public void run() {
            while (!stop) {

                LinkedList<MessageId> list = new LinkedList<MessageId>();
                markSuccessQueue.drainTo(list);

                for (final MessageId msgId : list) {
                    final BlockingQueue<Boolean> resultQueue = markSuccessStateTable.get(msgId);
                    OffsetState offsetState = headerLookupTable.get(msgId);
                    if (resultQueue != null && offsetState != null) {
                        headerChannel.write(ByteBuffer.allocate(4).putInt(MessageState.SUCCESS.ordinal()), offsetState.offset + 28, resultQueue, new CompletionHandler<Integer, BlockingQueue<Boolean>>() {
                            @Override
                            public void completed(Integer result, BlockingQueue<Boolean> attachment) {
                                headerLookupTable.remove(msgId);
                                put(resultQueue, true);
                            }

                            @Override
                            public void failed(Throwable exc, BlockingQueue<Boolean> attachment) {
                                put(resultQueue, false);
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

        public OffsetState(long offset, long bodyOffset, int bodyLength, MessageState state) {
            this.offset = offset;
            this.bodyOffset = bodyOffset;
            this.bodyLength = bodyLength;
            this.state = state;
        }
    }
}
