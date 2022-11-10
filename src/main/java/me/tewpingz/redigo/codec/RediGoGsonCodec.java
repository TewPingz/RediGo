package me.tewpingz.redigo.codec;

import com.google.gson.*;
import me.tewpingz.redigo.RediGo;
import me.tewpingz.redigo.data.RediGoObject;
import me.tewpingz.redigo.data.RediGoValue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

public class RediGoGsonCodec<K, V extends RediGoObject<K, ?>> implements JsonSerializer<V>, JsonDeserializer<V> {

    private final RediGo redigo;
    private final Class<K> keyClass;
    private final Function<K, V> valueCreator;

    /**
     * Default constructor for the gson codec
     * @param redigo the bridge instance to get the most recent gson instance from
     * @param keyClass the key class to be able to decode and encode key
     * @param valueCreator the value creator to create default value when decoding
     */
    public RediGoGsonCodec(RediGo redigo, Class<K> keyClass, Function<K, V> valueCreator) {
        this.redigo = redigo;
        this.keyClass = keyClass;
        this.valueCreator = valueCreator;
    }

    @Override
    public JsonElement serialize(V value, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_id", value.getKey().toString());

        try {
            this.getKeysAndValues(value).forEach((key, object) -> {
                jsonObject.add(key, this.redigo.getGson().toJsonTree(object));
            });
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return jsonObject;
    }

    @Override
    public V deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        try {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonElement idElement = jsonObject.get("_id");
            K key = this.redigo.getGson().fromJson(idElement, this.keyClass);
            V value = this.valueCreator.apply(key);
            for (Field field : this.getFields(value)) {
                RediGoValue redigoValue = field.getAnnotation(RediGoValue.class);
                String fieldKey = redigoValue.key();
                if (jsonObject.has(fieldKey)) {
                    Object fieldValue = this.redigo.getGson().fromJson(jsonObject.get(fieldKey), field.getType());
                    field.set(value, fieldValue);
                }
            }
            return value;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> getKeysAndValues(V bridgeObject) throws IllegalAccessException {
        Map<String, Object> serialized = new HashMap<>();

        for (Field field : this.getFields(bridgeObject)) {
            RediGoValue redigoValue = field.getAnnotation(RediGoValue.class);
            String key = redigoValue.key();
            Object value = field.get(bridgeObject);
            serialized.put(key, value);
        }

        return serialized;
    }

    private Collection<Field> getFields(V bridgeObject) {
        Collection<Field> fields = new HashSet<>();

        for (Field field : bridgeObject.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(RediGoValue.class)) {
                if ((field.getModifiers() & Modifier.FINAL) == Modifier.FINAL) {
                    continue;
                }
                field.setAccessible(true);
                fields.add(field);
            }
        }

        return fields;
    }
}
