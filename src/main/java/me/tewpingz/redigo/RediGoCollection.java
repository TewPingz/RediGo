package me.tewpingz.redigo;

import me.tewpingz.redigo.codec.RediGoGsonCodec;
import me.tewpingz.redigo.codec.RediGoRedissonCodec;
import org.redisson.api.*;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class RediGoCollection<K, V extends RediGoObject<K>> {
    private static final String LOCK_PREFIX = "LOCK_";

    private final RediGo redigo;
    private final String namespace;
    private final Function<K, V> valueCreator;

    // Redis caches
    private final int defaultTtl;
    private final RMapCache<K, V> redisMap;
    private final Map<K, V> localCache;
    private final RTopic topic;

    protected RediGoCollection(RediGo redigo, String namespace, Class<K> keyClass, Class<V> valueClass, int defaultTtl, Function<K, V> valueCreator) {
        this.redigo = redigo;
        this.namespace = namespace;
        this.valueCreator = valueCreator;
        this.defaultTtl = defaultTtl;
        this.localCache = new ConcurrentHashMap<>();

        // Set up our GSON decoder for the value class
        RediGoGsonCodec<K, V> gsonCodec = new RediGoGsonCodec<>(redigo, keyClass, valueCreator);
        redigo.setGson(redigo.getGson().newBuilder().registerTypeAdapter(valueClass, gsonCodec).create());

        // Set up our redis map with persistence
        RediGoMongoStorage<K, V> redigoMongoStorage = new RediGoMongoStorage<>(redigo, valueClass, namespace);
        RedissonClient redissonClient = this.redigo.getRedissonClient();
        RediGoRedissonCodec<K, V> codec = new RediGoRedissonCodec<>(this.redigo, valueClass);
        MapOptions<K, V> options = MapOptions.<K, V>defaults().loader(redigoMongoStorage).writer(redigoMongoStorage).writeMode(MapOptions.WriteMode.WRITE_THROUGH);
        this.redisMap = redissonClient.getMapCache(namespace, codec, options);

        // Update listener
        this.topic = redissonClient.getTopic(namespace, codec);
        this.topic.addListener(valueClass, (charSequence, value) -> {
            /*
                Update value only if value is already existing. We only want to update changes to the currently cached value
                We do not want to cache it if the current server does not want it to be cached.
             */
            if (this.localCache.containsKey(value.getKey())) {
                this.localCache.put(value.getKey(), value);
            }
        });
    }

    /**
     * Function to begin caching data locally
     * @param key the key of the data to start caching.
     */
    public void beginCachingLocally(K key) {
        Objects.requireNonNull(key);
        this.localCache.put(key, this.getOrCreateValueConcurrently(key));
    }

    /**
     * Function to begin caching data locally asynchronously
     * @param key the key of the data to start caching
     * @return a completable future to track if the task is complete or when it is.
     */
    public CompletableFuture<Void> beginCachingLocallyAsync(K key) {
        return CompletableFuture.runAsync(() -> this.beginCachingLocally(key));
    }

    /**
     * Function to stop caching the data locally
     * @param key the key of the data to stop caching.
     */
    public void stopCachingLocally(K key) {
        Objects.requireNonNull(key);
        this.localCache.remove(key);
    }

    /**
     * Get the cached value (This will not be the most recent but the best for main threaded tasks)
     * @param key the key of the data to get
     * @return the cached value.
     */
    public V getCachedValued(K key) {
        Objects.requireNonNull(key);
        return this.localCache.get(key);
    }

    /**
     * Function to get data with locks to ensure concurrency
     * This means the data returned by this object is the most recent data on the servers
     * @param key the key of the data to find
     * @return the most recent data from the redis server
     */
    public V getOrCreateValueConcurrently(K key) {
        Objects.requireNonNull(key);

        return this.ensureConcurrency(key, () -> {
            V value = this.redisMap.get(key);

            // This means the value doesn't exist in the database
            if (value == null) {
                value = this.valueCreator.apply(key);
                this.redisMap.fastPut(key, value, defaultTtl, TimeUnit.MINUTES);
            }

            return value;
        });
    }

    /**
     * Function to get data with locks to ensure concurrency asynchronously
     * This means the data returned by this object is the most recent data on the servers
     * @param key the key of the data to find
     * @return completable future for with the data
     */
    public CompletableFuture<V> getOrCreateDataConcurrentlyAsync(K key) {
        return CompletableFuture.supplyAsync(() -> this.getOrCreateValueConcurrently(key));
    }

    /**
     * Function to update data real time
     * @param key the key of the data being changed
     * @param consumer the consumer that will be called when the change should be queued
     */
    public void updateDataConcurrently(K key, Consumer<V> consumer) {
        Objects.requireNonNull(key);

        this.topic.publish(this.ensureConcurrency(key, () -> {
            V value = this.redisMap.getOrDefault(key, valueCreator.apply(key));
            consumer.accept(value);
            this.redisMap.fastPut(key, value, this.defaultTtl, TimeUnit.MINUTES);
            return value;
        }));
    }

    /**
     * Function to update data real time asynchronously
     * @param key the key of the data being changed
     * @param consumer the consumer that will be called when the change should be queued
     */
    public CompletableFuture<Void> updateDataConcurrentlyAsync(K key, Consumer<V> consumer) {
        return CompletableFuture.runAsync(() -> this.updateDataConcurrently(key, consumer));
    }

    /**
     * Function to ensure concurrency when changing values
     * @param key the key of the value that will be changed
     * @param supplier the supplier that will be called between the locks
     * @return the value returned from the supplier.
     * @param <T> The type of data being returned from the supplier.
     */
    private  <T> T ensureConcurrency(K key, Supplier<T> supplier) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(supplier);

        RLock lock = this.getLock(key);
        T value = null;

        try {
            // Use a timeout function to ensure we do not have a deadlock (image)
            lock.lock(30, TimeUnit.SECONDS);
            value = supplier.get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

        return value;
    }

    private RLock getLock(K key) {
        return this.redigo.getRedissonClient().getLock(LOCK_PREFIX + this.namespace + key.toString());
    }
}
