package me.tewpingz.redigo.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import me.tewpingz.redigo.RediGo;
import me.tewpingz.redigo.data.RediGoObject;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;

public class RediGoRedissonCodec<K, V extends RediGoObject<K>> extends BaseCodec {

    @Getter
    private final RediGo redigo;
    private final GsonEncoder<K, V> encoder;
    private final GsonDecoder<K, V> decoder;

    /**
     * Bridge redisson codec
     * @param redigo the bridge instance to get most recent gson
     * @param valueClass the value class to be able to decode the value back to the class
     */
    public RediGoRedissonCodec(RediGo redigo, Class<V> valueClass) {
        this.redigo = redigo;
        this.encoder = new GsonEncoder<>(this);
        this.decoder = new GsonDecoder<>(this, valueClass);
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return this.decoder;
    }

    @Override
    public Encoder getValueEncoder() {
        return this.encoder;
    }

    @Override
    public ClassLoader getClassLoader() {
        return super.getClassLoader();
    }

    @RequiredArgsConstructor
    private static class GsonDecoder<K, V extends RediGoObject<K>> implements Decoder<Object> {

        private final RediGoRedissonCodec<K, V> codec;
        private final Class<V> valueClass;

        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            try (ByteBufInputStream stream = new ByteBufInputStream(buf)) {
                String gson = stream.readUTF();
                return this.codec.getRedigo().getGson().fromJson(gson, valueClass);
            }
        }
    }

    @RequiredArgsConstructor
    private static class GsonEncoder<K, V extends RediGoObject<K>> implements Encoder {

        private final RediGoRedissonCodec<K, V> codec;

        @Override
        public ByteBuf encode(Object in) throws IOException {
            ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
            try {
                ByteBufOutputStream os = new ByteBufOutputStream(out);
                os.writeUTF(this.codec.getRedigo().getGson().toJson(in));
                return os.buffer();
            } catch (IOException e) {
                out.release();
                throw e;
            } catch (Exception e) {
                out.release();
                throw new IOException(e);
            }
        }
    }
}
