package com.alibaba.middleware.race.rpc.api.codec;

import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Dawnwords on 2015/7/27.
 */
public class KryoSerializer implements SerializerFactory {

    private final Kryo kryo = new KryoReflectionFactorySupport();
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override
    public ByteToMessageDecoder deserializer() {
        return new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4) {
            @Override
            protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
                ByteBuf frame = null;
                Object result = null;
                try {
                    frame = (ByteBuf) super.decode(ctx, in);
                    if (null == frame) {
                        return null;
                    }

                    result = kryo.readClassAndObject(new Input(new ByteBufInputStream(frame)));
                } catch (Exception e) {
                    Logger.error(e);
                } finally {
                    if (null != frame) {
                        frame.release();
                    }
                }

                return result;
            }
        };
    }

    @Override
    public MessageToByteEncoder serializer() {
        return new MessageToByteEncoder() {
            @Override
            protected void encode(ChannelHandlerContext ctx, Object obj, ByteBuf byteBuf) throws Exception {
                try {
                    ByteBufOutputStream out = new ByteBufOutputStream(byteBuf);
                    out.write(LENGTH_PLACEHOLDER);
                    Output output = new Output(out);
                    kryo.writeClassAndObject(output, obj);
                } catch (Exception e) {
                    Logger.error(e);
                    ctx.channel().close();
                }
            }
        };
    }
}
