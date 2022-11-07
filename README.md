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
    Redigo redigo = new Redigo("namespace", mongoClient, redissonClient, gson);
```

#### Create a RediGo object
```java
    @AllArgsConstructor
    @RequiredArgsConstructor
    public class TestObject implements RedigoObject<String> {

        private final String key;

        @RedigoValue(key = "thisIsTheKeyInTheObject")
        private String name;

        @Override
        public String getKey() {
            return key;
        }
    }
```

#### Create a RediGo collection
```java
    RedigoCollection<String, TestObject> collection = redigo.createCollection("namespace", String.class, TestObject.class, 30, TestObject::new);
```

#### Get a RediGo real time object from the collection
```java
    TestObject object = collection.getOrCreateValueConcurrently("itemKey");
```

#### Update a RediGo real time object from the collection
```java
    collection.updateDataConcurrently("itemKey", bridgeObj -> {
        bridgeObj.name = "HelloWorld";
    });
```
#### This is an example setup of RediGo
```java
    public class Main {
        public static void main(String[] args) {
            RedissonClient redissonClient = Redisson.create();
            MongoClient mongoClient = MongoClients.create();
            Gson gson = new GsonBuilder().create();
    
            Redigo redigo = new Redigo("namespace", mongoClient, redissonClient, gson);
            RedigoCollection<String, TestObject> collection = redigo.createCollection("namespace", String.class, TestObject.class, 30, TestObject::new);
    
            collection.updateDataConcurrently("itemKey", bridgeObj -> {
                bridgeObj.name = "HelloWorld";
            });
        }
    
        @AllArgsConstructor
        @RequiredArgsConstructor
        public static class TestObject implements RedigoObject<String> {
    
            private final String key;
    
            @RedigoValue(key = "thisIsTheKeyInTheObject")
            private String name;
    
            @Override
            public String getKey() {
                return key;
            }
        }
    }
```
