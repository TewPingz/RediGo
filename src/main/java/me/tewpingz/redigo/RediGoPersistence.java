package me.tewpingz.redigo;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import lombok.Getter;
import me.tewpingz.redigo.codec.RediGoMongoCodec;
import me.tewpingz.redigo.data.RediGoObject;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.redisson.api.AsyncIterator;
import org.redisson.api.map.MapLoader;
import org.redisson.api.map.MapWriterAsync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Getter
public class RediGoPersistence<K, V extends RediGoObject<K, ?>> implements MapWriterAsync<K, V>, MapLoader<K, V> {
    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    private final RediGo redigo;
    private final Class<V> valueClass;
    private final MongoCollection<V> mongoCollection;

    /**
     * Default constructor for the bridge mongo storage for our to ensure persistence
     * @param redigo the bridge instance for most recent gson
     * @param valueClass the value class to pass into the codecs
     * @param namespace the namespace of the storage (Basically the collection name)
     */
    protected RediGoPersistence(RediGo redigo, Class<V> valueClass, String namespace) {
        this.redigo = redigo;
        this.valueClass = valueClass;

        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(new RediGoMongoCodec<>(this.redigo, this.valueClass)),
                MongoClientSettings.getDefaultCodecRegistry());
        MongoCollection<V> collection = redigo.getMongoClient().getDatabase(redigo.getNamespace()).getCollection(namespace, valueClass);
        this.mongoCollection = collection.withCodecRegistry(codecRegistry);
    }

    @Override
    public V load(K key) {
        return this.mongoCollection.find(Filters.eq(key.toString())).first();
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return this.mongoCollection.find().map(RediGoObject::getKey);
    }


    @Override
    public CompletionStage<Void> write(Map<K, V> map) {
        return CompletableFuture.runAsync(() -> {
            List<ReplaceOneModel<V>> bulk = new ArrayList<>();
            map.values().forEach(value -> bulk.add(new ReplaceOneModel<>(Filters.eq(value.getKey().toString()), value, REPLACE_OPTIONS)));
            if (bulk.isEmpty()) {
                return;
            }
            this.mongoCollection.bulkWrite(bulk);
        });
    }

    @Override
    public CompletionStage<Void> delete(Collection<K> keys) {
        return CompletableFuture.runAsync(() -> {
            List<DeleteOneModel<V>> bulk = new ArrayList<>();
            keys.forEach(key -> bulk.add(new DeleteOneModel<>(Filters.eq(key.toString()))));
            if (bulk.isEmpty()) {
                return;
            }
            this.mongoCollection.bulkWrite(bulk);
        });
    }
}
