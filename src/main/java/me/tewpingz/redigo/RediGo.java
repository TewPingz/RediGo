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

    /**
     * A function that allows you to create a {@link RediGoCollection} for your {@link RediGoObject}
     * with a common constructor
     *
     * @param namespace the namespace of the {@link RediGoCollection}
     * @param keyClass the key class of the {@link RediGoObject}
     * @param valueClass the value class of {@link RediGoObject}
     * @param defaultTtl the amount of minutes to store the {@link RediGoObject} inside the redis database
     * @param defaultCaching whether of not to cache all the values by default
     * @param creator the creator that is called when either initialized the object for the first time or deserialization
     * @return a {@link RediGoCollection} that is for your {@link RediGoObject}
     * @param <S> the reference to the {@link RediGoObject.Snapshot}
     * @param <K> the reference to the key that is used for {@link RediGoObject}
     * @param <V> the reference that is used for {@link RediGoObject}
     */
    public <S extends RediGoObject.Snapshot, K, V extends RediGoObject<K, S>> RediGoCollection<S, K, V> createCollection(
            String namespace, Class<K> keyClass, Class<V> valueClass, int defaultTtl, boolean defaultCaching, Function<K, V> creator) {
        return new RediGoCollection<>(this, namespace, keyClass, valueClass, defaultTtl, defaultCaching, creator, creator);
    }

    /**
     * A function that allows you to create a {@link RediGoCollection} for your {@link RediGoObject}
     * with separate constructors for the initialization and the deserialization
     *
     * @param namespace the namespace of the {@link RediGoCollection}
     * @param keyClass the key class of the {@link RediGoObject}
     * @param valueClass the value class of {@link RediGoObject}
     * @param defaultTtl the amount of minutes to store the {@link RediGoObject} inside the redis database
     * @param defaultCaching whether of not to cache all the values by default
     * @param initialCreator the creator that is used to create the object if it was not found in the database. The initial time
     * @param emptyCreator the creator that is used while deserializing the object.
     * @return a {@link RediGoCollection} that is for your {@link RediGoObject}
     * @param <S> the reference to the {@link RediGoObject.Snapshot}
     * @param <K> the reference to the key that is used for {@link RediGoObject}
     * @param <V> the reference that is used for {@link RediGoObject}
     */
    public <S extends RediGoObject.Snapshot, K, V extends RediGoObject<K, S>> RediGoCollection<S, K, V> createCollection(
            String namespace, Class<K> keyClass, Class<V> valueClass, int defaultTtl, boolean defaultCaching, Function<K, V> initialCreator, Function<K, V> emptyCreator) {
        return new RediGoCollection<>(this, namespace, keyClass, valueClass, defaultTtl, defaultCaching, initialCreator, emptyCreator);
    }
}
