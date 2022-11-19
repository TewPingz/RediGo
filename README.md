# RediGo

A way to ensure concurrency when editing data through multiple instance. 
It uses data to communicate with data and stores information in redis for the
desired amount of time. But don't worry, when the data is no longer in redis it uses
MongoDB to fetch the last model that was left by the redis instance.

### TLDR
* Uses MongoDB to persist data
* Uses Redis to store recently changed data for the amount you set
* Uses Redis locks to allow you to get the most recent version of data and ONLY allows you to edit the most recent versions of data so you can never override with old data
* This will fix issues where data may not be the most recent when switching to difference instances quickly

## How to use RediGo

#### Create RediGo instance
```java
    new RediGo("core", mongoClient, redissonClient, gson);
```

#### Create a RediGo object
```java
@Data
@RequiredArgsConstructor
public class AltEntry implements RediGoObject<String, AltEntry.AltProfileSnapshot> {

    private final String hashedIp;

    @RediGoValue(key = "relatedIds")
    private Set<UUID> relatedIds = new HashSet<>();

    public void addUuid(UUID uuid) {
        this.relatedIds.add(uuid);
    }

    @Override
    public String getKey() {
        return this.hashedIp;
    }

    @Override
    public AltProfileSnapshot getSnapshot() {
        return new AltProfileSnapshot(this.hashedIp, this.relatedIds);
    }

    @Data
    @RequiredArgsConstructor
    public static class AltProfileSnapshot implements Snapshot {
        private final String hashedIp;
        private final Set<UUID> relatedIds;
    }
}

```

#### Create a RediGo collection
```java
   redigoInstance.createCollection("alts", String.class, AltEntry.class, 30, false, AltEntry::new);
```

#### Get a RediGo real time object from the collection
```java
    public AltEntry.AltProfileSnapshot getAlts(String hashedIp) {
        return this.collection.getOrCreateRealValue(hashedIp);
    }
```

#### Update a RediGo real time object from the collection
```java
    public AltEntry.AltProfileSnapshot addUuid(String hashedIp, UUID uuid) {
        return this.collection.updateRealValue(hashedIp, altEntry -> altEntry.addUuid(uuid));
    }
```
#### This is an example setup of RediGo
```java
public class AltManager {

    private final RediGoCollection<AltEntry.AltProfileSnapshot, String, AltEntry> collection;

    public AltManager(Core instance) {
        this.collection = instance.getRediGo().createCollection("alts", String.class, AltEntry.class, 30, false, AltEntry::new);
    }

    public AltEntry.AltProfileSnapshot addUuid(String hashedIp, UUID uuid) {
        return this.collection.updateRealValue(hashedIp, altEntry -> altEntry.addUuid(uuid));
    }

    public AltEntry.AltProfileSnapshot getAlts(String hashedIp) {
        return this.collection.getOrCreateRealValue(hashedIp);
    }
}
```

```java
@Data
@RequiredArgsConstructor
public class AltEntry implements RediGoObject<String, AltEntry.AltProfileSnapshot> {

    private final String hashedIp;

    @RediGoValue(key = "relatedIds")
    private Set<UUID> relatedIds = new HashSet<>();

    public void addUuid(UUID uuid) {
        this.relatedIds.add(uuid);
    }

    @Override
    public String getKey() {
        return this.hashedIp;
    }

    @Override
    public AltProfileSnapshot getSnapshot() {
        return new AltProfileSnapshot(this.hashedIp, this.relatedIds);
    }

    @Data
    @RequiredArgsConstructor
    public static class AltProfileSnapshot implements Snapshot {
        private final String hashedIp;
        private final Set<UUID> relatedIds;
    }
}
```
