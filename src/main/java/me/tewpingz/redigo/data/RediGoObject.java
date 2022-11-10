package me.tewpingz.redigo.data;

public interface RediGoObject<K, S extends RediGoObject.Snapshot> {

    K getKey();

    S getSnapshot();

    interface Snapshot {

    }
}
