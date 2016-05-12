package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;

public final class Cmds {

  public static final Cmd<Object> AUTH = Cmd.create("AUTH");

  public static final Cmd<Object> CLIENT = Cmd.create("CLIENT");
  public static final Cmd<String> GETNAME = Cmd.create("GETNAME", RESP::toString);
  public static final Cmd<Object> KILL = Cmd.create("KILL");
  public static final Cmd<Object> ID = Cmd.create("ID");
  public static final Cmd<Object> TYPE = Cmd.create("TYPE");
  public static final Cmd<Object> ADDR = Cmd.create("ADDR");
  public static final Cmd<Object> SKIPME = Cmd.create("SKIPME");
  public static final Cmd<String> LIST = Cmd.create("LIST", RESP::toString);
  public static final Cmd<Object> PAUSE = Cmd.create("PAUSE");
  public static final Cmd<Object> REPLY = Cmd.create("REPLY");
  public static final Cmd<Object> ON = Cmd.create("ON");
  public static final Cmd<Object> OFF = Cmd.create("OFF");
  public static final Cmd<Object> SKIP = Cmd.create("SKIP");
  public static final Cmd<Object> SETNAME = Cmd.create("SETNAME");

  public static final Cmd<Object> ECHO = Cmd.create("ECHO");
  public static final Cmd<Object> PING = Cmd.create("PING");
  public static final Cmd<Object> QUIT = Cmd.create("QUIT");
  public static final Cmd<Object> SELECT = Cmd.create("SELECT");
  public static final Cmd<Long> DEL = Cmd.create("DEL", d -> (Long) d);
  public static final Cmd<Object> EXISTS = Cmd.create("EXISTS");
  public static final Cmd<Object> KEYS = Cmd.create("KEYS");
  public static final Cmd<Object> SORT = Cmd.create("SORT");
  public static final Cmd<Object> MIGRATE = Cmd.create("MIGRATE");
  public static final Cmd<Object> MOVE = Cmd.create("MOVE");
  public static final Cmd<Object> OBJECT = Cmd.create("OBJECT");
  public static final Cmd<Object> PERSIST = Cmd.create("PERSIST");
  public static final Cmd<Object> RANDOMKEY = Cmd.create("RANDOMKEY");
  public static final Cmd<Object> DUMP = Cmd.create("DUMP");
  public static final Cmd<Object> RESTORE = Cmd.create("RESTORE");
  public static final Cmd<Object> RENAME = Cmd.create("RENAME");
  public static final Cmd<Object> RENAMENX = Cmd.create("RENAMENX");
  public static final Cmd<Object> RENAMEX = Cmd.create("RENAMEX");
  public static final Cmd<Object> DBSIZE = Cmd.create("DBSIZE");
  public static final Cmd<Object> SHUTDOWN = Cmd.create("SHUTDOWN");
  public static final Cmd<Object> INFO = Cmd.create("INFO");
  public static final Cmd<Object> MONITOR = Cmd.create("MONITOR");
  public static final Cmd<Object> CONFIG = Cmd.create("CONFIG");
  public static final Cmd<Object> SLOWLOG = Cmd.create("SLOWLOG");
  public static final Cmd<Object> SAVE = Cmd.create("SAVE");
  public static final Cmd<Object> BGSAVE = Cmd.create("BGSAVE");
  public static final Cmd<Object> BGREWRITEAOF = Cmd.create("BGREWRITEAOF");
  public static final Cmd<Object> LASTSAVE = Cmd.create("LASTSAVE");
  public static final Cmd<Object> FLUSHDB = Cmd.create("FLUSHDB");
  public static final Cmd<Object> FLUSHALL = Cmd.create("FLUSHALL");

  public static final Cmd<Object> EXPIRE = Cmd.create("EXPIRE");
  public static final Cmd<Object> EXPIREAT = Cmd.create("EXPIREAT");
  public static final Cmd<Object> TTL = Cmd.create("TTL");
  public static final Cmd<Object> PEXPIRE = Cmd.create("PEXPIRE");
  public static final Cmd<Object> PEXPIREAT = Cmd.create("PEXPIREAT");
  public static final Cmd<Object> PTTL = Cmd.create("PTTL");

  public static final Cmd<Object> BITCOUNT = Cmd.create("BITCOUNT");
  public static final Cmd<Object> BITOP = Cmd.create("BITOP");
  public static final Cmd<Object> SETBIT = Cmd.create("SETBIT");
  public static final Cmd<Object> GETBIT = Cmd.create("GETBIT");
  public static final Cmd<Object> BITPOS = Cmd.create("BITPOS");
  public static final Cmd<Object> BITFIELD = Cmd.create("BITFIELD");

  public static final Cmd<Object> SET = Cmd.create("SET");
  public static final Cmd<Object> GET_RAW = Cmd.create("GET");
  public static final Cmd<String> GET = Cmd.create("GET", RESP::toString);
  public static final Cmd<Object> GETSET = Cmd.create("GETSET");
  public static final Cmd<Object> MGET = Cmd.create("MGET");
  public static final Cmd<Object> MSET = Cmd.create("MSET");
  public static final Cmd<Object> MSETNX = Cmd.create("MSETNX");
  public static final Cmd<Object> DECRBY = Cmd.create("DECRBY");
  public static final Cmd<Object> DECR = Cmd.create("DECR");
  public static final Cmd<Object> INCRBY = Cmd.create("INCRBY");
  public static final Cmd<Object> INCR = Cmd.create("INCR");
  public static final Cmd<Object> INCRBYFLOAT = Cmd.create("INCRBYFLOAT");

  public static final Cmd<Object> APPEND = Cmd.create("APPEND");
  public static final Cmd<Object> STRLEN = Cmd.create("STRLEN");
  public static final Cmd<Object> SETRANGE = Cmd.create("SETRANGE");
  public static final Cmd<Object> GETRANGE = Cmd.create("GETRANGE");

  public static final Cmd<Object> HSET = Cmd.create("HSET");
  public static final Cmd<Object> HGET = Cmd.create("HGET");
  public static final Cmd<Object> HSETNX = Cmd.create("HSETNX");
  public static final Cmd<Object> HMSET = Cmd.create("HMSET");
  public static final Cmd<Object> HMGET = Cmd.create("HMGET");
  public static final Cmd<Object> HKEYS = Cmd.create("HKEYS");
  public static final Cmd<Object> HVALS = Cmd.create("HVALS");
  public static final Cmd<Object> HGETALL = Cmd.create("HGETALL");
  public static final Cmd<Object> HEXISTS = Cmd.create("HEXISTS");
  public static final Cmd<Object> HDEL = Cmd.create("HDEL");
  public static final Cmd<Object> HLEN = Cmd.create("HLEN");
  public static final Cmd<Object> HINCRBY = Cmd.create("HINCRBY");
  public static final Cmd<Object> HINCRBYFLOAT = Cmd.create("HINCRBYFLOAT");

  public static final Cmd<Object> LINSERT = Cmd.create("LINSERT");
  public static final Cmd<Object> LPUSHX = Cmd.create("LPUSHX");
  public static final Cmd<Object> LPUSH = Cmd.create("LPUSH");
  public static final Cmd<Object> LLEN = Cmd.create("LLEN");
  public static final Cmd<Object> LRANGE = Cmd.create("LRANGE");
  public static final Cmd<Object> LTRIM = Cmd.create("LTRIM");
  public static final Cmd<Object> LINDEX = Cmd.create("LINDEX");
  public static final Cmd<Object> LSET = Cmd.create("LSET");
  public static final Cmd<Object> LREM = Cmd.create("LREM");
  public static final Cmd<Object> LPOP = Cmd.create("LPOP");

  public static final Cmd<Object> RPUSHX = Cmd.create("RPUSHX");
  public static final Cmd<Object> RPUSH = Cmd.create("RPUSH");
  public static final Cmd<Object> RPOP = Cmd.create("RPOP");
  public static final Cmd<Object> RPOPLPUSH = Cmd.create("RPOPLPUSH");
  public static final Cmd<Object> BLPOP = Cmd.create("BLPOP");
  public static final Cmd<Object> BRPOP = Cmd.create("BRPOP");
  public static final Cmd<Object> BRPOPLPUSH = Cmd.create("BRPOPLPUSH");

  public static final Cmd<Object> SADD = Cmd.create("SADD");
  public static final Cmd<Object> SMEMBERS = Cmd.create("SMEMBERS");
  public static final Cmd<Object> SREM = Cmd.create("SREM");
  public static final Cmd<Object> SPOP = Cmd.create("SPOP");
  public static final Cmd<Object> SMOVE = Cmd.create("SMOVE");
  public static final Cmd<Long> SCARD = Cmd.create("SCARD", d -> (Long) d);
  public static final Cmd<Object> SISMEMBER = Cmd.create("SISMEMBER");
  public static final Cmd<Object> SRANDMEMBER = Cmd.create("SRANDMEMBER");
  public static final Cmd<Object> SINTER = Cmd.create("SINTER");
  public static final Cmd<Object> SINTERSTORE = Cmd.create("SINTERSTORE");
  public static final Cmd<Object> SUNION = Cmd.create("SUNION");
  public static final Cmd<Object> SUNIONSTORE = Cmd.create("SUNIONSTORE");
  public static final Cmd<Object> SDIFF = Cmd.create("SDIFF");
  public static final Cmd<Object> SDIFFSTORE = Cmd.create("SDIFFSTORE");

  public static final Cmd<Object> ZADD = Cmd.create("ZADD");
  public static final Cmd<Object> ZCOUNT = Cmd.create("ZCOUNT");
  public static final Cmd<Object> ZLEXCOUNT = Cmd.create("ZLEXCOUNT");
  public static final Cmd<Object> ZCARD = Cmd.create("ZCARD");
  public static final Cmd<Object> ZSCORE = Cmd.create("ZSCORE");
  public static final Cmd<Object[]> ZRANGE = Cmd.create("ZRANGE", d -> (Object[]) d);
  public static final Cmd<Object> ZREVRANGE = Cmd.create("ZREVRANGE");
  public static final Cmd<Object> ZREM = Cmd.create("ZREM");
  public static final Cmd<Object> ZRANK = Cmd.create("ZRANK");
  public static final Cmd<Object> ZREMRANGEBYRANK = Cmd.create("ZREMRANGEBYRANK");
  public static final Cmd<Object> ZREVRANK = Cmd.create("ZREVRANK");
  public static final Cmd<Object> ZRANGEBYSCORE = Cmd.create("ZRANGEBYSCORE");
  public static final Cmd<Object> ZREVRANGEBYSCORE = Cmd.create("ZREVRANGEBYSCORE");
  public static final Cmd<Object> ZREMRANGEBYSCORE = Cmd.create("ZREMRANGEBYSCORE");
  public static final Cmd<Object> ZRANGEBYLEX = Cmd.create("ZRANGEBYLEX");
  public static final Cmd<Object> ZREVRANGEBYLEX = Cmd.create("ZREVRANGEBYLEX");
  public static final Cmd<Object> ZREMRANGEBYLEX = Cmd.create("ZREMRANGEBYLEX");
  public static final Cmd<Object> ZUNIONSTORE = Cmd.create("ZUNIONSTORE");
  public static final Cmd<Object> ZINTERSTORE = Cmd.create("ZINTERSTORE");

  public static final Cmd<Object> MULTI = Cmd.create("MULTI");
  public static final Cmd<Object> DISCARD = Cmd.create("DISCARD");
  public static final Cmd<Object> EXEC = Cmd.create("EXEC");
  public static final Cmd<Object> WATCH = Cmd.create("WATCH");
  public static final Cmd<Object> UNWATCH = Cmd.create("UNWATCH");
  public static final Cmd<Object> SYNC = Cmd.create("SYNC");

  public static final Cmd<Object> SUBSCRIBE = Cmd.create("SUBSCRIBE");
  public static final Cmd<Object> PUBLISH = Cmd.create("PUBLISH");
  public static final Cmd<Object> UNSUBSCRIBE = Cmd.create("UNSUBSCRIBE");
  public static final Cmd<Object> PSUBSCRIBE = Cmd.create("PSUBSCRIBE");
  public static final Cmd<Object> PUNSUBSCRIBE = Cmd.create("PUNSUBSCRIBE");
  public static final Cmd<Object> PUBSUB = Cmd.create("PUBSUB");

  public static final Cmd<Object> MODULE = Cmd.create("MODULE");

  public static final Cmd<Object> SCAN = Cmd.create("SCAN");
  public static final Cmd<Object> HSCAN = Cmd.create("HSCAN");
  public static final Cmd<Object> SSCAN = Cmd.create("SSCAN");
  public static final Cmd<Object> ZSCAN = Cmd.create("ZSCAN");

  public static final Cmd<Object> PFADD = Cmd.create("PFADD");
  public static final Cmd<Object> PFCOUNT = Cmd.create("PFCOUNT");
  public static final Cmd<Object> PFMERGE = Cmd.create("PFMERGE");

  public static final Cmd<Object> GEOADD = Cmd.create("GEOADD");
  public static final Cmd<Object> GEODIST = Cmd.create("GEODIST");
  public static final Cmd<Object> GEOHASH = Cmd.create("GEOHASH");
  public static final Cmd<Object> GEOPOS = Cmd.create("GEOPOS");
  public static final Cmd<Object> GEORADIUS = Cmd.create("GEORADIUS");
  public static final Cmd<Object> GEORADIUSBYMEMBER = Cmd.create("GEORADIUSBYMEMBER");
}
