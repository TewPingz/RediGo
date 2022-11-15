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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class RediGoCollection<S extends RediGoObject.Snapshot, K, V extends RediGoObject<K, S>> {
    private static final String LOCK_PREFIX = "LOCK_";

    private final RediGo redigo;
    private final String redisNamespace;
    private final Function<K, V> initialCreator;

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

    protected RediGoCollection(RediGo redigo, String namespace, Class<K> keyClass, Class<V> valueClass,
                               int defaultTtl, boolean defaultCaching, Function<K, V> initialCreator, Function<K, V> emptyCreator) {
        this.redigo = redigo;
        this.redisNamespace = redigo.getNamespace() + "_" + namespace;
        this.initialCreator = initialCreator;
        this.defaultTtl = defaultTtl;
        this.defaultCaching = defaultCaching;
        this.localCache = new ConcurrentHashMap<>();

        /*
          Set up our GSON decoder for the value class
          Use empty creator to ensure you don't fetch any values from external apis, just make an empty object to append onto
         */
        RediGoGsonCodec<K, V> gsonCodec = new RediGoGsonCodec<>(redigo, keyClass, emptyCreator);
        redigo.setGson(redigo.getGson().newBuilder().registerTypeAdapter(valueClass, gsonCodec).create());

        // Set up our redis map with persistence
        this.persistence = new RediGoPersistence<>(redigo, valueClass, namespace);
        RedissonClient redissonClient = this.redigo.getRedissonClient();
        RediGoRedissonCodec<K, V> codec = new RediGoRedissonCodec<>(this.redigo, valueClass);
        MapOptions<K, V> options = MapOptions.<K, V>defaults().loader(this.persistence).writer(this.persistence).writeMode(MapOptions.WriteMode.WRITE_THROUGH);
        this.redisMap = redissonClient.getMapCache(this.redisNamespace, codec, options);

        // Create listener
        this.createTopic = redissonClient.getTopic("%s_create_topic".formatted(this.redisNamespace), codec);
        this.createTopic.addListener(valueClass, (channel, value) -> {
            if (this.defaultCaching) {
                this.localCache.put(value.getKey(), value.getSnapshot());
            }
        });

        // Update listener
        this.updateTopic = redissonClient.getTopic("%s_update_topic".formatted(this.redisNamespace), codec);
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
     * This function will make a get request from the database to get the latest value
     * It is necessary you call this function asynchronously or call {@link RediGoCollection#beginCachingLocallyAsync(K)}
     *
     * @param key the key of the data to begin caching for.
     */
    public void beginCachingLocally(K key) {
        Objects.requireNonNull(key);
        this.localCache.put(key, this.getOrCreateRealValue(key));
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
     * Function to stop caching the data locally, this will ensure that any new requests that update the value
     * are ignored and stop caching the value in the map. Make sure that {@link RediGoCollection#defaultCaching} is false
     * otherwise this will cause an error as default caching is enabled it shouldn't allow you to uncache anything
     *
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
     * A function that allows you to get the current cached value of the key provided.
     * Keep in mind this is not the latest value this is the latest cached value.
     * Look at {@link RediGoCollection#getOrCreateRealValue(K)} for the real value
     * and if you are looking to update the value look at {@link RediGoCollection#updateRealValue(K, Consumer)}
     *
     * @param key the key of the object that you would like.
     * @return a snapshot of the latest cached object.
     */
    public S getCachedValued(K key) {
        Objects.requireNonNull(key);
        return this.localCache.get(key);
    }

    /**
     * A function that allows you to get all the {@link RediGoObject.Snapshot} values inside the map.
     * This allows you to count the amount of entries there are and loop through it.
     * If you are only looking to just look through it, look at {@link RediGoCollection#forEachCachedValue(Consumer)}
     *
     * @return a collection of the latest cached {@link RediGoObject.Snapshot} of the values cached.
     */
    public Collection<S> getCachedValues() {
        return this.localCache.values();
    }

    /**
     * A function that allows you to apply a consumer
     * to all {@link RediGoObject.Snapshot} values inside the cached map.
     *
     * @param consumer the consumer to execute with the value
     */
    public void forEachCachedValue(Consumer<S> consumer) {
        Objects.requireNonNull(consumer);
        this.getCachedValues().forEach(consumer);
    }

    /**
     * Function to get data with locks to ensure reliability of {@link RediGoObject}
     * This means the data returned by {@link RediGoObject} is the most recent data on the servers
     *
     * @param key the key of the data to find
     * @return a {@link RediGoObject.Snapshot} of the latest value.
     */
    public S getOrCreateRealValue(K key) {
        Objects.requireNonNull(key);

        AtomicBoolean created = new AtomicBoolean(false);

        V fetchedValue = this.executeSafely(key, () -> {
            V value = this.redisMap.get(key);

            // This means the value doesn't exist in the database
            if (value == null) {
                value = this.initialCreator.apply(key);
                this.redisMap.fastPut(key, value, defaultTtl, TimeUnit.MINUTES);
                created.set(true);
            }

            return value;
        });

        if (created.get()) {
            this.createTopic.publish(fetchedValue);
        }

        return fetchedValue.getSnapshot();
    }

    /**
     * Function to get data with locks to ensure reliability of {@link RediGoObject}
     * This means the data returned by {@link RediGoObject} is the most recent data on the servers
     * This function is called asynchronously
     *
     * @param key the key of the data to find
     * @return a completable future with a {@link RediGoObject.Snapshot} of the latest value.
     */
    public CompletableFuture<S> getOrCreateRealValueAsync(K key) {
        return CompletableFuture.supplyAsync(() -> this.getOrCreateRealValue(key));
    }

    /**
     * A function that allows you to update the data of {@link RediGoObject} by making sure the update
     * is being applied to the latest version of the {@link RediGoObject}. This means
     * data can never be overwritten or lost simply because all the data that is being applied
     * is always to the real value.
     *
     * @param key the key of the object to get and update
     * @param consumer the consumer that will be called to change the value
     * @return a snapshot of the latest value with the changes applied.
     */
    public S updateRealValue(K key, Consumer<V> consumer) {
        Objects.requireNonNull(key);

        V fetchedValue = this.executeSafely(key, () -> {
            V value = this.redisMap.getOrDefault(key, initialCreator.apply(key));
            consumer.accept(value);
            this.redisMap.fastPut(key, value, this.defaultTtl, TimeUnit.MINUTES);
            return value;
        });

        this.updateTopic.publish(fetchedValue);
        return fetchedValue.getSnapshot();
    }

    /**
     * A function that allows you to update the data of {@link RediGoObject} by making sure the update
     * is being applied to the latest version of the {@link RediGoObject}. This means
     * data can never be overwritten or lost simply because all the data that is being applied
     * is always to the real value.
     * This function is called asynchronously.
     *
     * @param key the key of the object to get and update
     * @param consumer the consumer that will be called to change the value
     * @return a completable future with a snapshot of the latest value with the changes applied.
     */
    public CompletableFuture<S> updateRealValueAsync(K key, Consumer<V> consumer) {
        return CompletableFuture.supplyAsync(() -> this.updateRealValue(key, consumer));
    }

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
        return this.redigo.getRedissonClient().getLock(LOCK_PREFIX + this.redisNamespace + key.toString());
    }
}
