##Jedipus [![Build Status](https://img.shields.io/travis/jamespedwards42/jedipus.svg?branch=master)](https://travis-ci.org/jamespedwards42/jedipus) [![Bintray](https://api.bintray.com/packages/jamespedwards42/libs/jedipus/images/download.svg) ](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion) [![license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/jamespedwards42/jedipus/master/LICENSE) [![Gitter Chat](https://badges.gitter.im/jamespedwards42/jedipus.svg)](https://gitter.im/jamespedwards42/jedipus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

>Jedipus is a Redis Cluster Java client that manages [Jedis](https://github.com/xetorthio/jedis) object pools.

######Features
* Reuses the awesome work already done on Jedis by supporting all super interfaces of [`Jedis`](https://github.com/xetorthio/jedis/blob/master/src/main/java/redis/clients/jedis/Jedis.java), [`Pipeline`](https://github.com/xetorthio/jedis/blob/master/src/main/java/redis/clients/jedis/Pipeline.java) and [`Transaction`](https://github.com/xetorthio/jedis/blob/master/src/main/java/redis/clients/jedis/Transaction.java).
* Execute `Consumer<IJedis>` and `Function<IJedis, R>` lambas.
* Direct O(1) primitive array access to a corresponding [`IJedis`](src/main/java/com/fabahaba/jedipus/IJedis.java) pool.
* Reuse known slot integers.
* Locking is only applied to threads that are accessing slots that are moving, there is no known node, or for which a client connection continually cannot be established, triggering a slot cache refresh.
* Minimal dependencies, Jedis and org.apache.commons:commons-pool2.
* Optional user supplied [`ClusterNode`](src/main/java/com/fabahaba/jedipus/cluster/ClusterNode.java) -> `ObjectPool<IJedis>` factories.
* Load balance read-only requests across pools.  Optional user supplied `ObjectPool<IJedis>[]` -> [`LoadBalancedPools`](src/main/java/com/fabahaba/jedipus/cluster/LoadBalancedPools.java) factories.  By default, a [round robin strategy](src/main/java/com/fabahaba/jedipus/cluster/RoundRobinPools.java) is used.
* [Client side HostPort mapping to internally networked clusters](https://gist.github.com/jamespedwards42/5037cf03768280ab1d81a88e7929c608).
* Configurable retry delay per cluster node for `JedisConnectionException's`.
* Execute against known or random nodes.
* Utilities to manage and execute Lua scripts.

######Read Modes
>Read modes control how pools to master and slave nodes are managed.

* MASTER: Only pools to master nodes are maintained.  
* SLAVES: Only pools to slave nodes are maintained. Calls are load balanced across slave pools.
* MIXED_SLAVES: Pools are managed for both masters and slave nodes.  Calls are only load balanced across slave pools. Individual calls can be overridden with `ReadMode.MASTER` or `ReadMode.MIXED`.  When no slave pools are available the master pool is used.
* MIXED: Pools are managed for both masters and slave nodes.  Calls are load balanced across both master and slave pools. Individual calls can be overridden with `ReadMode.MASTER` or `ReadMode.SLAVES`.  When overriding with `ReadMode.SLAVES` and no slave pools are available the master pool is used.

#####Dependency Management
######Gradle
```groovy
repositories {
   jcenter()
}

dependencies {
   // Needed if supplying your own pool factories.
   // compile 'org.apache.commons:commons-pool2:+'
   compile 'com.fabahaba:jedipus:+'
}
```

#####Basic Usage Demos
```java
try (final JedisClusterExecutor jce =
      JedisClusterExecutor.startBuilding(ClusterNode.create("localhost", 7000)).create()) {

   final String key = "42";
   jce.acceptJedis(key, jedis -> jedis.set(key, "107.6"));

   final String temp = jce.applyJedis(key, jedis -> jedis.get(key));
   if (temp.equals("107.6")) {
      System.out.println("Showers' ready, don't forget your towel.");
   }
}
```

```java
try (final JedisClusterExecutor jce =
      JedisClusterExecutor.startBuilding(ClusterNode.create("localhost", 7000)).create()) {

   final String skey = "skey";

   final long numMembers = jce.applyPipeline(skey, pipeline -> {
      pipeline.sadd(skey, "member");
      final Response<Long> response = pipeline.scard(skey);
      pipeline.sync();
      return response.get();
   });

   System.out.format("'%s' has %d members.%n", skey, numMembers);
}
```

```java
try (final JedisClusterExecutor jce =
      JedisClusterExecutor.startBuilding(ClusterNode.create("localhost", 7000))
         .withReadMode(ReadMode.MIXED_SLAVES).create()) {

   // Ping-Pong all masters.
   jce.acceptAllMasters(master -> System.out.format("%s %s%n", master, master.ping()));

   // Ping-Pong all slaves concurrently.
   jce.applyAllSlaves(slave -> String.format("%s %s%n", slave, slave.ping()), 1,
      ForkJoinPool.commonPool()).stream().map(CompletableFuture::join)
      .forEach(System.out::println);

   // Hash tagged pipelined transaction.
   final String hashTag = RCUtils.createNameSpacedHashTag("HT");
   final int slot = JedisClusterCRC16.getSlot(hashTag);

   final String hashTaggedKey = hashTag + "key";
   final String fooKey = hashTag + "foo";

   final Set<Tuple> zrangeResult =
   jce.applyPipelinedTransaction(ReadMode.MASTER, slot, pipeline -> {

      pipeline.set(hashTaggedKey, "value");
      pipeline.zadd(fooKey, -1, "barowitch");
      pipeline.zadd(fooKey, .37, "barinsky");
      pipeline.zadd(fooKey, 42, "barikoviev");

      final Response<String> valueResponse = pipeline.get(hashTaggedKey);
      final Response<Set<Tuple>> bars = pipeline.zrangeWithScores(fooKey, 0, -1);

      // Note: Pipelines and transactions are merely started by the the library.
      // 'exec' and 'sync' must be called by the user.
      pipeline.exec();
      pipeline.sync();

      // Note: Responses must be captured within this lambda closure in order to properly
      // leverage error handling.

      // '{HT}:key': value
      System.out.format("%n'%s': %s%n", hashTaggedKey, valueResponse.get());

      return bars.get();
   });

   final String values = zrangeResult.stream()
      .map(tuple -> String.format("%s (%s)", tuple.getElement(), tuple.getScore()))
      .collect(Collectors.joining(", "));

   // '{HT}:foo': [barowitch (-1.0), barinsky (0.37), barikoviev (42.0)]
   System.out.format("%n'%s': [%s]%n", fooKey, values);

   // Read from load balanced slave.
   final String roResult =
   jce.applyJedis(ReadMode.SLAVES, slot, jedis -> jedis.get(hashTaggedKey));
   System.out.format("%n'%s': %s%n", hashTaggedKey, roResult);

   // cleanup
   final long numRemoved =
   jce.applyJedis(ReadMode.MASTER, slot, jedis -> jedis.del(hashTaggedKey, fooKey));
   System.out.format("%nRemoved %d keys.%n", numRemoved);
}
```

#####Lua Redis Lock

```java
public final class RedisLock {

  private RedisLock() {}

   private static final LuaScript<List<Object>> TRY_ACQUIRE_LOCK =
     LuaScript.fromResourcePath("/TRY_ACQUIRE_LOCK.lua");

   private static final LuaScript<byte[]> TRY_RELEASE_LOCK =
     LuaScript.fromResourcePath("/TRY_RELEASE_LOCK.lua");

   public static void main(final String[] args) {

      final Collection<ClusterNode> discoveryNodes =
         Collections.singleton(ClusterNode.create("localhost", 7000));

      try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

         LuaScript.loadMissingScripts(jce, TRY_ACQUIRE_LOCK, TRY_RELEASE_LOCK);

         final byte[] lockName = RESP.toBytes("mylock");
         final byte[] ownerId = RESP.toBytes("myOwnerId");
         final byte[] pexpire = RESP.toBytes(1000);

         final List<Object> lockOwners = TRY_ACQUIRE_LOCK.eval(jce, 1, lockName, ownerId, pexpire);

         // final byte[] previousOwner = (byte[]) lockOwners.get(0);
         final byte[] currentOwner = (byte[]) lockOwners.get(1);
         final long pttl = (long) lockOwners.get(2);

         // 'myOwnerId' has lock 'mylock' for 1000ms.
         System.out.format("'%s' has lock '%s' for %dms.%n", RESP.toString(currentOwner),
             RESP.toString(lockName), pttl);

         final byte[] tryReleaseOwner = TRY_RELEASE_LOCK.eval(jce, 1, lockName, ownerId);

         if (tryReleaseOwner != null && Arrays.equals(tryReleaseOwner, ownerId)) {
           // Lock was released by 'myOwnerId'.
           System.out.format("Lock was released by '%s'.%n", RESP.toString(ownerId));
         } else {
           System.out.format("Lock was no longer owned by '%s'.%n", RESP.toString(ownerId));
         }
      }
   }
}
```

**src/main/resoures/TRY_ACQUIRE_LOCK.lua**
```lua
-- Returns the previous owner, the current owner and the pttl for the lock.
-- Returns either {null, lockOwner, pexpire}, {owner, owner, pexpire} or {owner, owner, pttl}.
-- The previous owner is null if 'lockOwner' newly acquired the lock. Otherwise, the previous
--   owner will be same value as the current owner. If the current owner is equal to the supplied
--   'lockOwner' argument then the ownership claim will remain active for 'pexpire' milliseconds.

local lockName = KEYS[1];
local lockOwner = ARGV[1];

local owner = redis.call('get', lockName);

if not owner or owner == lockOwner then

   local px = tonumber(ARGV[2]);

   redis.call('set', lockName, lockOwner, 'PX', px);

   return {owner, lockOwner, px};
end

return {owner, owner, redis.call('pttl', lockName)};
```

**src/main/resoures/TRY_RELEASE_LOCK.lua**
```lua
-- Returns the current owner at the time of this call.
-- The 'lockName' key is deleted if the requesting owner matches the current.

local lockName = KEYS[1];
local lockOwner = ARGV[1];

local currentOwner = redis.call('get', lockName);

if lockOwner == currentOwner then
   redis.call('del', lockName);
end

return currentOwner;
```
