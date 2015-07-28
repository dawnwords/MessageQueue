package com.alibaba.middleware.race.rpc.api.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import de.javakaffee.kryoserializers.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
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

    @Override
    public ByteToMessageDecoder deserializer() {
        return new ByteToMessageDecoder() {
            boolean finishHeader = false;
            int bodyLength;

            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
                if (!finishHeader) {
                    if (in.readableBytes() < 4) {
                        return;
                    }
                    bodyLength = in.readInt();
                    finishHeader = true;
                } else {
                    if (in.readableBytes() < bodyLength) {
                        return;
                    }
                    byte[] body = new byte[bodyLength];
                    in.readBytes(body);
                    Input input = new Input(body);
                    Object obj = kryo.get().readClassAndObject(input);
                    input.close();
                    out.add(obj);
                    finishHeader = false;
                }
            }
        };
    }

    @Override
    public MessageToByteEncoder serializer() {
        return new MessageToByteEncoder() {
            @Override
            protected void encode(ChannelHandlerContext ctx, Object obj, ByteBuf out) throws Exception {
                ByteOutputStream bos = new ByteOutputStream();
                Output output = new Output(bos);
                kryo.get().writeClassAndObject(output, obj);
                output.close();
                byte[] body = bos.getBytes();
                out.writeInt(body.length);
                out.writeBytes(body);
            }
        };
    }
}
