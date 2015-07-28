package com.alibaba.middleware.race.rpc.api.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.javakaffee.kryoserializers.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.lang.reflect.InvocationHandler;
import java.util.*;

/**
 * Created by Dawnwords on 2015/7/27.
 */
public class KryoSerializer implements SerializerFactory {

    private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
            kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
            kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
            kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
            kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
            kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
            kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
            kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
            kryo.register(InvocationHandler.class, new JdkProxySerializer());
            UnmodifiableCollectionsSerializer.registerSerializers(kryo);
            SynchronizedCollectionsSerializer.registerSerializers(kryo);

            return kryo;
        }
    };

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override
    public ByteToMessageDecoder deserializer() {
        return new LengthFieldBasedFrameDecoder(10485760, 0, 4, 0, 4) {
            @Override
            protected Object decode(final ChannelHandlerContext ctx, final ByteBuf in) throws Exception {
                ByteBuf frame = (ByteBuf) super.decode(ctx, in);
                if (frame == null) {
                    return null;
                }
                try {
                    ByteBufInputStream is = new ByteBufInputStream(frame);
                    Input input = new Input(is);
                    Object result = kryo.get().readClassAndObject(input);
                    input.close();
                    return result;
                } finally {
                    frame.release();
                }
            }
        };
    }

    @Override
    public MessageToByteEncoder serializer() {
        return new MessageToByteEncoder() {
            @Override
            protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
                int startIdx = out.writerIndex();
                ByteBufOutputStream bout = new ByteBufOutputStream(out);
                bout.write(LENGTH_PLACEHOLDER);
                Output output = new Output(bout);
                kryo.get().writeClassAndObject(output, msg);
                output.close();
                int endIdx = out.writerIndex();
                out.setInt(startIdx, endIdx - startIdx - 4);
            }
        };
    }
}
