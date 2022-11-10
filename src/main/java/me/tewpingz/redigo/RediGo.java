package me.tewpingz.redigo;

import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import lombok.Data;
import me.tewpingz.redigo.data.RediGoObject;
import org.redisson.api.RedissonClient;

import java.util.function.Function;

@Data
public class RediGo {

    private final String namespace;
    private final MongoClient mongoClient;
    private final RedissonClient redissonClient;

    private Gson gson;

    public RediGo(String namespace, MongoClient mongoClient, RedissonClient redissonClient, Gson gson) {
        this.namespace = namespace;
        this.mongoClient = mongoClient;
        this.redissonClient = redissonClient;
        this.gson = gson.newBuilder().disableHtmlEscaping().create();
    }

    public <S extends RediGoObject.Snapshot, K, V extends RediGoObject<K, S>> RediGoCollection<S, K, V> createCollection(
            String namespace, Class<K> keyClass, Class<V> valueClass, int defaultTtl, boolean defaultCaching, Function<K, V> valueCreator) {
        return new RediGoCollection<>(this, namespace, keyClass, valueClass, defaultTtl, defaultCaching, valueCreator);
    }
}
