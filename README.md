##Jedipus [![Build Status](https://img.shields.io/travis/jamespedwards42/jedipus.svg?branch=master)](https://travis-ci.org/jamespedwards42/jedipus) [![Bintray](https://api.bintray.com/packages/jamespedwards42/libs/jedipus/images/download.svg) ](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion) [![license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/jamespedwards42/jedipus/master/LICENSE) [![Gitter Chat](https://badges.gitter.im/jamespedwards42/jedipus.svg)](https://gitter.im/jamespedwards42/jedipus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

>Jedipus is a Redis Cluster Java client that manages client object pools and command execution.

######Features
* Execute `Consumer<RedisClient>` and `Function<RedisClient, R>` lambas.
* Flexible generic or primitive return types to match the dynamic return type design of Redis.
* Flexible command interface allows for calling [Modules](https://github.com/antirez/redis/blob/unstable/src/modules/API.md) or renamed commands.
* Performance focused:
  * Reuse known slot integers for direct O(1) primitive array access to a corresponding `RedisClient` pool.
  * Minimal enforced (de)serialization.  Write directly to the socket output stream buffer, and retrieve raw responses.
  * Official Fire-And-Forget support using [`CLIENT REPLY ON|OFF|SKIP`](http://redis.io/commands/client-reply).
  * Locking is only applied to threads which are accessing slots that are migrating; there is no known node; or for which a client connection continually cannot be established; all of which will trigger a slot cache refresh.
  * Primitive long, long[] and long[][] return types to avoid auto boxing, nice for BITFIELD commands.
  * Load balance read-only requests across master and/or slave pools.
* Single dependency on `org.apache.commons:commons-pool2:+`.
* PGP signed releases.  [Bintray](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion) verifies signatures automatically.  See [verifying your Jedipus jar](scripts/gpgVerifyJedipus.sh).
* Optional user supplied [`Node`](src/main/java/com/fabahaba/jedipus/cluster/Node.java) -> `ObjectPool<RedisClient>` factories.
* Optional user supplied [`LoadBalancedPools`](src/main/java/com/fabahaba/jedipus/concurrent/LoadBalancedPools.java) factories.  By default, a [round robin strategy](src/main/java/com/fabahaba/jedipus/cluster/RoundRobinPools.java) is used.
* [Client side HostPort mapping](https://gist.github.com/jamespedwards42/5037cf03768280ab1d81a88e7929c608) to internally-networked clusters.
* Configurable `RedisConnectionException` [retry delays](src/main/java/com/fabahaba/jedipus/concurrent/ElementRetryDelay.java) per cluster node.  By default, an [exponential back-off delay](src/main/java/com/fabahaba/jedipus/concurrent/SemaphoredRetryDelay.java) is used.
* Execute directly against known or random nodes.
* Utilities to manage and execute Lua scripts, see this [RedisLock Gist](https://gist.github.com/jamespedwards42/46bc6fcd6e2c81315d2d63a4e80b527f).
* Frequent point releases for new features, utilities and bug fixes.

######Read Modes
>Read modes control how pools to master and slave nodes are managed.

* MASTER: Only pools to master nodes are maintained.  
* SLAVES: Only pools to slave nodes are maintained. Calls are load balanced across slave pools.
* MIXED_SLAVES: Pools are managed for both masters and slave nodes.  Calls are only load balanced across slave pools. Individual calls can be overridden with `ReadMode.MASTER` or `ReadMode.MIXED`.  When no slave pools are available the master pool is used.
* MIXED: Pools are managed for both masters and slave nodes.  Calls are load balanced across both master and slave pools. Individual calls can be overridden with `ReadMode.MASTER` or `ReadMode.SLAVES`.  When overriding with `ReadMode.SLAVES` and no slave pools are available the master pool is used.

######Dependency Management
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
try (final RedisClusterExecutor rce =
    RedisClusterExecutor.startBuilding(Node.create("localhost", 7000)).create()) {

  final String key = "42";
  // skip() issues a pipelined CLIENT REPLY SKIP
  rce.accept(key, client -> client.skip().sendCmd(Cmds.SET, key, "107.6"));

  final String temp = rce.apply(key, client -> client.sendCmd(Cmds.GET, key));
  if (temp.equals("107.6")) {
    System.out.println("Showers' ready, don't forget your towel.");
  }
}
```

```java
try (final RedisClusterExecutor rce =
    RedisClusterExecutor.startBuilding(Node.create("localhost", 7000)).create()) {

  final String skey = "skey";

  final FutureLongReply numMembers = rce.applyPipeline(skey, pipeline -> {
    // Fire and forget SADD.
    pipeline.skip().sendCmd(Cmds.SADD, skey, "member");
    // Optional primitive return types.
    final FutureLongReply response = pipeline.sendCmd(Cmds.SCARD.prim(), skey);
    pipeline.sync();
    // Check reply to leverage library error handling.
    return response.checkReply();
  });

  // This long was never auto boxed.
  final long primitiveNumMembers = numMembers.getLong();
  System.out.format("'%s' has %d members.%n", skey, primitiveNumMembers);
}
```

```java
try (final RedisClusterExecutor rce =
    RedisClusterExecutor.startBuilding(Node.create("localhost", 7000))
        .withReadMode(ReadMode.MIXED_SLAVES).create()) {

  // Hash tagged pipelined transaction.
  final String hashTag = RCUtils.createNameSpacedHashTag("HT");
  final int slot = CRC16.getSlot(hashTag);

  final String hashTaggedKey = hashTag + "key";
  final String fooKey = hashTag + "foo";

  // Implicit multi applied.
  final Object[] sortedBars = rce.applyPipelinedTransaction(ReadMode.MASTER, slot, pipeline -> {

    pipeline.sendCmd(Cmds.SET, hashTaggedKey, "value");
    pipeline.sendCmd(Cmds.ZADD, fooKey, "NX", "-1", "barowitch");
    pipeline.sendCmd(Cmds.ZADD, fooKey, "XX", "-2", "barowitch");
    // Handle different ZADD return types with flexible command design.
    pipeline.sendCmd(Cmds.ZADD_INCR, fooKey, "XX", "INCR", "-1", "barowitch");
    // Utilities to avoid extra array creation.
    pipeline.sendCmd(Cmds.ZADD, ZAddParams.fillNX(new byte[][] {RESP.toBytes(fooKey), null,
        RESP.toBytes(.37), RESP.toBytes("barinsky")}));
    pipeline.sendCmd(Cmds.ZADD, fooKey, "42", "barikoviev");

    final FutureReply<String> valueResponse = pipeline.sendCmd(Cmds.GET, hashTaggedKey);
    final FutureReply<Object[]> barsResponse =
        pipeline.sendCmd(Cmds.ZRANGE, fooKey, "0", "-1", "WITHSCORES");

    // Note: Pipelines and transactions (multi) are merely started by the the library.
    // 'exec' and 'sync' must be called by the user.
    pipeline.execSync();

    // Note: Responses must be captured within this lambda closure in order to properly
    // leverage error handling.

    // '{HT}:key': value
    System.out.format("'%s': %s%n", hashTaggedKey, valueResponse.get());

    return barsResponse.get();
  });

  // '{HT}:foo': [barowitch (-1.0), barinsky (0.37), barikoviev (42.0)]
  System.out.format("%n'%s':", fooKey);
  for (int i = 0; i < sortedBars.length;) {
    System.out.format(" %s (%s)", RESP.toString(sortedBars[i++]),
        RESP.toDouble(sortedBars[i++]));
  }

  // Read from load balanced slave.
  final String roResult =
      rce.apply(ReadMode.SLAVES, slot, client -> client.sendCmd(Cmds.GET, hashTaggedKey));
  System.out.format("%n'%s': %s%n", hashTaggedKey, roResult);

  // Optional primitive return types; no auto boxing!
  final long numRemoved = rce.apply(ReadMode.MASTER, slot,
      client -> client.sendCmd(Cmds.DEL.prim(), hashTaggedKey, fooKey));
  System.out.format("%nRemoved %d keys.%n", numRemoved);
}
```

```java
try (final RedisClusterExecutor rce =
    RedisClusterExecutor.startBuilding(Node.create("localhost", 7000))
        .withReadMode(ReadMode.MIXED_SLAVES).create()) {

  // Ping-Pong all masters.
  rce.acceptAllMasters(
      master -> System.out.format("%s from %s%n", master.sendCmd(Cmds.PING), master.getNode()));

  // Ping-Pong all slaves concurrently.
  rce.applyAllSlaves(
      slave -> String.format("%s from %s", slave.sendCmd(Cmds.PING, "Howdy"), slave.getNode()), 1,
      ForkJoinPool.commonPool()).stream().map(CompletableFuture::join)
      .forEach(System.out::println);
}
```
