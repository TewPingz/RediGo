package me.tewpingz.redigo;

import me.tewpingz.redigo.codec.RediGoGsonCodec;
import me.tewpingz.redigo.codec.RediGoRedissonCodec;
import me.tewpingz.redigo.data.RediGoObject;
import org.redisson.api.*;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class RediGoCollection<S extends RediGoObject.Snapshot, K, V extends RediGoObject<K, S>> {
    private static final String LOCK_PREFIX = "LOCK_";

    private final RediGo redigo;
    private final String namespace;
    private final Function<K, V> valueCreator;

    // Configurations
    private final boolean defaultCaching;
    private final int defaultTtl;

    // Redis caches
    private final RediGoPersistence<K, V> persistence;
    private final RMapCache<K, V> redisMap;

    // Redis local caches
    private final Map<K, S> localCache;
    private final RTopic createTopic;
    private final RTopic updateTopic;

    protected RediGoCollection(RediGo redigo, String namespace, Class<K> keyClass, Class<V> valueClass, int defaultTtl, boolean defaultCaching, Function<K, V> valueCreator) {
        this.redigo = redigo;
        this.namespace = namespace;
        this.valueCreator = valueCreator;
        this.defaultTtl = defaultTtl;
        this.defaultCaching = defaultCaching;
        this.localCache = new ConcurrentHashMap<>();

        // Set up our GSON decoder for the value class
        RediGoGsonCodec<K, V> gsonCodec = new RediGoGsonCodec<>(redigo, keyClass, valueCreator);
        redigo.setGson(redigo.getGson().newBuilder().registerTypeAdapter(valueClass, gsonCodec).create());

        // Set up our redis map with persistence
        this.persistence = new RediGoPersistence<>(redigo, valueClass, namespace);
        RedissonClient redissonClient = this.redigo.getRedissonClient();
        RediGoRedissonCodec<K, V> codec = new RediGoRedissonCodec<>(this.redigo, valueClass);
        MapOptions<K, V> options = MapOptions.<K, V>defaults().loader(this.persistence).writer(this.persistence).writeMode(MapOptions.WriteMode.WRITE_THROUGH);
        this.redisMap = redissonClient.getMapCache(namespace, codec, options);

        // Create listener
        this.createTopic = redissonClient.getTopic("%s_create_topic".formatted(this.namespace), codec);
        this.createTopic.addListener(valueClass, (channel, value) -> {
            if (this.defaultCaching) {
                this.localCache.put(value.getKey(), value.getSnapshot());
            }
        });

        // Update listener
        this.updateTopic = redissonClient.getTopic("%s_update_topic".formatted(this.namespace), codec);
        this.updateTopic.addListener(valueClass, (charSequence, value) -> {
            if (this.defaultCaching || this.localCache.containsKey(value.getKey())) {
                this.localCache.put(value.getKey(), value.getSnapshot());
            }
        });

        // Cache all values if default caching is enabled
        if (this.defaultCaching) {
            this.persistence.loadAllKeys().forEach(this::beginCachingLocally);
        }
    }

    /**
     * Function to begin caching data locally
     * @param key the key of the data to start caching.
     */
    public void beginCachingLocally(K key) {
        Objects.requireNonNull(key);
        this.localCache.put(key, this.getOrCreateRealValue(key).getSnapshot());
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
        if (this.defaultCaching) {
            throw  new IllegalStateException("Tried to uncache a value in a collection that has default caching enabled.");
        }
        Objects.requireNonNull(key);
        this.localCache.remove(key);
    }

    /**
     * Get the cached value (This will not be the most recent but the best for main threaded tasks)
     * @param key the key of the data to get
     * @return the cached value.
     */
    public S getCachedValued(K key) {
        Objects.requireNonNull(key);
        return this.localCache.get(key);
    }

    /**
     * A function to get all the cached values in the map
     * @return a collection of all the cached values.
     */
    public Collection<S> getCachedValues() {
        return this.localCache.values();
    }

    /**
     * Loop through the currently cached items
     * @param consumer the consumer to execute with the value
     */
    public void forEachCachedValue(Consumer<S> consumer) {
        Objects.requireNonNull(consumer);
        this.getCachedValues().forEach(consumer);
    }

    /**
     * Function to get data with locks to ensure concurrency
     * This means the data returned by this object is the most recent data on the servers
     * @param key the key of the data to find
     * @return the most recent data from the redis server
     */
    public V getOrCreateRealValue(K key) {
        Objects.requireNonNull(key);

        AtomicBoolean created = new AtomicBoolean(false);

        V fetchedValue = this.executeSafely(key, () -> {
            V value = this.redisMap.get(key);

            // This means the value doesn't exist in the database
            if (value == null) {
                value = this.valueCreator.apply(key);
                this.redisMap.fastPut(key, value, defaultTtl, TimeUnit.MINUTES);
                created.set(true);
            }

            return value;
        });

        if (created.get()) {
            this.createTopic.publish(fetchedValue);
        }

        return fetchedValue;
    }

    /**
     * Function to get data with locks to ensure concurrency asynchronously
     * This means the data returned by this object is the most recent data on the servers
     * @param key the key of the data to find
     * @return completable future for with the data
     */
    public CompletableFuture<V> getOrCreateRealValueAsync(K key) {
        return CompletableFuture.supplyAsync(() -> this.getOrCreateRealValue(key));
    }

    /**
     * Function to update data real time
     * @param key the key of the data being changed
     * @param consumer the consumer that will be called when the change should be queued
     */
    public void updateRealValue(K key, Consumer<V> consumer) {
        Objects.requireNonNull(key);

        this.updateTopic.publish(this.executeSafely(key, () -> {
            V value = this.redisMap.getOrDefault(key, valueCreator.apply(key));
            consumer.accept(value);
            this.redisMap.fastPut(key, value, this.defaultTtl, TimeUnit.MINUTES);
            return value;
        }));
    }

    /**

     * Function to update data real time and get a return value
     * @param key the key of the data being changed
     * @param function the function that will be executed while updating the real value
     * @return the return value returned from the function
     * @param <T> the type of the return value from the function
     */
    public <T> T updateRealValueWithFunction(K key, Function<V, T> function) {
        Objects.requireNonNull(key);

        AtomicReference<T> returnValue = new AtomicReference<>(null);

        this.updateTopic.publish(this.executeSafely(key, () -> {
            V value = this.redisMap.getOrDefault(key, valueCreator.apply(key));
            returnValue.set(function.apply(value));
            this.redisMap.fastPut(key, value, this.defaultTtl, TimeUnit.MINUTES);
            return value;
        }));

        return returnValue.get();
    }

    /**
     * Function to update data real time asynchronously
     * @param key the key of the data being changed
     * @param consumer the consumer that will be called when the change should be queued
     */
    public CompletableFuture<Void> updateRealValueAsync(K key, Consumer<V> consumer) {
        return CompletableFuture.runAsync(() -> this.updateRealValue(key, consumer));
    }

    /**
     * Function to update data real time and get a return value asynchronously
     * @param key the key of the data being changed
     * @param function the consumer that will be called when the change should be queued
     * @return the return value returned from the function
     * @param <T> the type of the return value from the function
     */
    public <T> CompletableFuture<T> updateRealValueWithFunctionAsync(K key, Function<V, T> function) {
        return CompletableFuture.supplyAsync(() -> this.updateRealValueWithFunction(key, function));
    }

    /**
     * Function to ensure safety when changing values
     * @param key the key of the value that will be changed
     * @param supplier the supplier that will be called between the locks
     * @return the value returned from the supplier.
     * @param <T> The type of data being returned from the supplier.
     */
    private  <T> T executeSafely(K key, Supplier<T> supplier) {
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
