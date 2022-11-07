package me.tewpingz.redigo.codec;

import com.google.gson.*;
import me.tewpingz.redigo.RediGo;
import me.tewpingz.redigo.data.RediGoObject;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigInteger;

public class RediGoMongoCodec<K, V extends RediGoObject<K>> implements Codec<V> {

    private final RediGo redigo;
    private final Class<V> valueClass;

    /**
     * Constructor for the mongo codec
     * @param redigo the bridge instance to get most recent gson
     * @param valueClass the value class to be able to decode the json back to the object
     */
    public RediGoMongoCodec(RediGo redigo, Class<V> valueClass) {
        this.redigo = redigo;
        this.valueClass = valueClass;
    }

    @Override
    public V decode(BsonReader bsonReader, DecoderContext decoderContext) {
        JsonObject object = this.readObject(bsonReader);
        return this.redigo.getGson().fromJson(object.toString(), this.valueClass);
    }

    @Override
    public void encode(BsonWriter bsonWriter, V value, EncoderContext encoderContext) {
        JsonObject object = this.redigo.getGson().toJsonTree(value).getAsJsonObject();
        this.writeJsonElement(object, bsonWriter);
    }

    @Override
    public Class<V> getEncoderClass() {
        return this.valueClass;
    }

    private void writeJsonElement(JsonElement jsonElement, BsonWriter writer) {
        if (jsonElement.isJsonObject()) {
            writer.writeStartDocument();
            jsonElement.getAsJsonObject().entrySet().forEach(entry -> {
                writer.writeName(entry.getKey());
                this.writeJsonElement(entry.getValue(), writer);
            });
            writer.writeEndDocument();
        } else if (jsonElement.isJsonPrimitive()) {
            JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
            if (jsonPrimitive.isString()) {
                writer.writeString(jsonPrimitive.getAsString());
            } else if (jsonPrimitive.isNumber()) {
                Number number = jsonElement.getAsNumber();
                if (number instanceof Double) {
                    writer.writeDouble(number.doubleValue());
                } else if (number instanceof Integer) {
                    writer.writeInt32(number.intValue());
                } else if (number instanceof Long) {
                    writer.writeInt64(number.longValue());
                } else if (number instanceof Float) {
                    writer.writeDouble(number.floatValue());
                } else if (number instanceof Short) {
                    writer.writeInt32(number.shortValue());
                } else if (number instanceof Byte) {
                    writer.writeInt32(number.byteValue());
                } else if (number instanceof BigInteger){
                    writer.writeInt64(number.longValue());
                }
            } else if (jsonPrimitive.isBoolean()) {
                writer.writeBoolean(jsonPrimitive.getAsBoolean());
            }
        } else if (jsonElement.isJsonNull()) {
            writer.writeNull();
        } else if (jsonElement.isJsonArray()) {
            writer.writeStartArray();
            jsonElement.getAsJsonArray().forEach(element -> this.writeJsonElement(element, writer));
            writer.writeEndArray();
        }
    }

    private JsonElement readElement(BsonReader reader) {
        BsonType keyType = reader.getCurrentBsonType();
        switch (keyType) {
            case DOCUMENT -> {
                return this.readObject(reader);
            }
            case NULL -> {
                reader.readNull();
                return new JsonNull();
            }
            case INT32 -> {
                return new JsonPrimitive(reader.readInt32());
            }
            case INT64 -> {
                return new JsonPrimitive(reader.readInt64());
            }
            case BOOLEAN -> {
                return new JsonPrimitive(reader.readBoolean());
            }
            case STRING -> {
                return new JsonPrimitive(reader.readString());
            }
            case ARRAY -> {
                return this.readArray(reader);
            }
            case DOUBLE -> {
                return new JsonPrimitive(reader.readDouble());
            }
            default -> {
                reader.skipValue();
                return new JsonNull();
            }
        }
    }

    private JsonArray readArray(BsonReader reader) {
        reader.readStartArray();
        JsonArray jsonArray = new JsonArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            jsonArray.add(this.readElement(reader));
        }
        reader.readEndArray();
        return jsonArray;
    }

    private JsonObject readObject(BsonReader reader) {
        JsonObject jsonObject = new JsonObject();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            jsonObject.add(key, this.readElement(reader));
        }
        reader.readEndDocument();
        return jsonObject;
    }
}
