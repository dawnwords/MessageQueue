package com.alibaba.middleware.race.mom.codec;

import com.alibaba.middleware.race.mom.bean.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.internal.RecyclableArrayList;
import io.netty.util.internal.StringUtil;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class Decoder extends ChannelInboundHandlerAdapter {

    private final int maxFrameLength = 10485760;
    private final int lengthFieldLength = 4;
    ByteBuf cumulation;
    private boolean decodeWasNull;
    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;
    private long bytesToDiscard;

    protected int actualReadableBytes() {
        return internalBuffer().readableBytes();
    }

    protected ByteBuf internalBuffer() {
        if (cumulation != null) {
            return cumulation;
        } else {
            return Unpooled.EMPTY_BUFFER;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        RecyclableArrayList out = RecyclableArrayList.newInstance();
        try {
            if (msg instanceof ByteBuf) {
                ByteBuf data = (ByteBuf) msg;
                if (cumulation == null) {
                    cumulation = data;
                    try {
                        callDecode(ctx, cumulation, out);
                    } finally {
                        if (cumulation != null && !cumulation.isReadable()) {
                            cumulation.release();
                            cumulation = null;
                        }
                    }
                } else {
                    try {
                        if (cumulation.writerIndex() > cumulation.maxCapacity() - data.readableBytes()) {
                            ByteBuf oldCumulation = cumulation;
                            cumulation = ctx.alloc().buffer(oldCumulation.readableBytes() + data.readableBytes());
                            cumulation.writeBytes(oldCumulation);
                            oldCumulation.release();
                        }
                        cumulation.writeBytes(data);
                        callDecode(ctx, cumulation, out);
                    } finally {
                        if (cumulation != null) {
                            if (!cumulation.isReadable()) {
                                cumulation.release();
                                cumulation = null;
                            } else {
                                cumulation.discardSomeReadBytes();
                            }
                        }
                        data.release();
                    }
                }
            } else {
                out.add(msg);
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Throwable t) {
            throw new DecoderException(t);
        } finally {
            if (out.isEmpty()) {
                decodeWasNull = true;
            }
            // batch transmission
            List<Object> results = new ArrayList<Object>();
            for (Object result : out) {
                results.add(result);
            }
            ctx.fireChannelRead(results);
            out.recycle();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (decodeWasNull) {
            decodeWasNull = false;
            if (!ctx.channel().config().isAutoRead()) {
                ctx.read();
            }
        }
        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        RecyclableArrayList out = RecyclableArrayList.newInstance();
        try {
            if (cumulation != null) {
                callDecode(ctx, cumulation, out);
                decodeLast(ctx, cumulation, out);
            } else {
                decodeLast(ctx, Unpooled.EMPTY_BUFFER, out);
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Exception e) {
            throw new DecoderException(e);
        } finally {
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
            for (Object o : out) {
                ctx.fireChannelRead(o);
            }
            ctx.fireChannelInactive();
        }
    }

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        ByteBuf buf = internalBuffer();
        int readable = buf.readableBytes();
        if (buf.isReadable()) {
            ByteBuf bytes = buf.readBytes(readable);
            buf.release();
            ctx.fireChannelRead(bytes);
        }
        cumulation = null;
        ctx.fireChannelReadComplete();
    }

    protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        try {
            while (in.isReadable()) {
                int outSize = out.size();
                int oldInputLength = in.readableBytes();
                decode(ctx, in, out);
                if (ctx.isRemoved()) break;
                if (outSize == out.size()) {
                    if (oldInputLength == in.readableBytes()) {
                        break;
                    } else {
                        continue;
                    }
                }

                if (oldInputLength == in.readableBytes()) {
                    throw new DecoderException(
                            StringUtil.simpleClassName(getClass()) +
                                    ".decode() did not read anything but decoded a message.");
                }
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Throwable cause) {
            throw new DecoderException(cause);
        }
    }

    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Object decoded = this.decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
    }

    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (this.discardingTooLongFrame) {
            long actualLengthFieldOffset = this.bytesToDiscard;
            int localBytesToDiscard = (int) Math.min(actualLengthFieldOffset, (long) in.readableBytes());
            in.skipBytes(localBytesToDiscard);
            actualLengthFieldOffset -= (long) localBytesToDiscard;
            this.bytesToDiscard = actualLengthFieldOffset;
            this.failIfNecessary(false);
        }

        int lengthFieldEndOffset = 4;
        if (in.readableBytes() < lengthFieldEndOffset) {
            return null;
        } else {
            int lengthFieldOffset = 0;
            int actualLengthFieldOffset1 = in.readerIndex() + lengthFieldOffset;
            long frameLength = this.getUnadjustedFrameLength(in, actualLengthFieldOffset1, this.lengthFieldLength, ByteOrder.BIG_ENDIAN);
            if (frameLength < 0L) {
                in.skipBytes(lengthFieldEndOffset);
                throw new CorruptedFrameException("negative pre-adjustment length field: " + frameLength);
            } else {
                int lengthAdjustment = 0;
                frameLength += (long) (lengthAdjustment + lengthFieldEndOffset);
                if (frameLength < (long) lengthFieldEndOffset) {
                    in.skipBytes(lengthFieldEndOffset);
                    throw new CorruptedFrameException("Adjusted frame length (" + frameLength + ") is less " + "than lengthFieldEndOffset: " + lengthFieldEndOffset);
                } else if (frameLength > (long) this.maxFrameLength) {
                    long frameLengthInt1 = frameLength - (long) in.readableBytes();
                    this.tooLongFrameLength = frameLength;
                    if (frameLengthInt1 < 0L) {
                        in.skipBytes((int) frameLength);
                    } else {
                        this.discardingTooLongFrame = true;
                        this.bytesToDiscard = frameLengthInt1;
                        in.skipBytes(in.readableBytes());
                    }

                    this.failIfNecessary(true);
                    return null;
                } else {
                    int frameLengthInt = (int) frameLength;
                    int initialBytesToStrip = 4;
                    if (in.readableBytes() < frameLengthInt) {
                        return null;
                    } else if (initialBytesToStrip > frameLengthInt) {
                        in.skipBytes(frameLengthInt);
                        throw new CorruptedFrameException("Adjusted frame length (" + frameLength + ") is less " + "than initialBytesToStrip: " + initialBytesToStrip);
                    } else {
                        in.skipBytes(initialBytesToStrip);
                        int readerIndex = in.readerIndex();
                        int actualFrameLength = frameLengthInt - initialBytesToStrip;
                        ByteBuf frame = this.extractFrame(ctx, in, readerIndex, actualFrameLength);
                        in.readerIndex(readerIndex + actualFrameLength);
                        return decode(frame);
                    }
                }
            }
        }
    }

    private Object decode(ByteBuf frame) {
        try {
            SerializeWrapper result;
            switch (frame.readByte()) {
                case SerializeWrapper.REGISTER:
                    result = new RegisterMessageWrapper().decode(frame);
                    break;
                case SerializeWrapper.MESSAGE:
                    result = new MessageWrapper().decode(frame);
                    break;
                case SerializeWrapper.SEND_RESULT:
                    result = new SendResultWrapper().decode(frame);
                    break;
                case SerializeWrapper.CONSUME_RESULT:
                    result = new ConsumeResultWrapper().decode(frame);
                    break;
                default:
                    result = null;
            }
            return result;
        } finally {
            frame.release();
        }
    }

    protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        buf = buf.order(order);
        long frameLength;
        switch (length) {
            case 1:
                frameLength = (long) buf.getUnsignedByte(offset);
                break;
            case 2:
                frameLength = (long) buf.getUnsignedShort(offset);
                break;
            case 3:
                frameLength = (long) buf.getUnsignedMedium(offset);
                break;
            case 4:
                frameLength = buf.getUnsignedInt(offset);
                break;
            case 5:
            case 6:
            case 7:
            default:
                throw new DecoderException("unsupported lengthFieldLength: " + this.lengthFieldLength + " (expected: 1, 2, 3, 4, or 8)");
            case 8:
                frameLength = buf.getLong(offset);
        }

        return frameLength;
    }

    private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
        if (this.bytesToDiscard == 0L) {
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0L;
            this.discardingTooLongFrame = false;
            if (firstDetectionOfTooLongFrame) {
                this.fail(tooLongFrameLength);
            }
        } else if (firstDetectionOfTooLongFrame) {
            this.fail(this.tooLongFrameLength);
        }

    }

    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        ByteBuf frame = ctx.alloc().buffer(length);
        frame.writeBytes(buffer, index, length);
        return frame;
    }

    private void fail(long frameLength) {
        if (frameLength > 0L) {
            throw new TooLongFrameException("Adjusted frame length exceeds " + this.maxFrameLength + ": " + frameLength + " - discarded");
        } else {
            throw new TooLongFrameException("Adjusted frame length exceeds " + this.maxFrameLength + " - discarding");
        }
    }

    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        decode(ctx, in, out);
    }

}
