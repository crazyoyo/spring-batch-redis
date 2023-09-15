package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.Struct.Type;

public enum KeyEvent {

    DEL,
    RENAME_FROM,
    RENAME_TO,
    MOVE_FROM,
    MOVE_TO,
    COPY_TO,
    RESTORE,
    EXPIRE,
    SORTSTORE,
    SET(Type.STRING),
    SETRANGE(Type.STRING),
    INCRBY(Type.STRING),
    INCRBYFLOAT(Type.STRING),
    APPEND(Type.STRING),
    LPUSH(Type.LIST),
    RPUSH(Type.LIST),
    RPOP(Type.LIST),
    LPOP(Type.LIST),
    LINSERT(Type.LIST),
    LSET(Type.LIST),
    LREM(Type.LIST),
    LTRIM(Type.LIST),
    HSET(Type.HASH),
    HINCRBY(Type.HASH),
    HINCRBYFLOAT(Type.HASH),
    HDEL(Type.HASH),
    SADD(Type.SET),
    SPOP(Type.SET),
    SINTERSTORE(Type.SET),
    SUNIONSTORE(Type.SET),
    SDIFFSTORE(Type.SET),
    ZINCR(Type.ZSET),
    ZADD(Type.ZSET),
    ZREM(Type.ZSET),
    ZREMBYSCORE(Type.ZSET),
    ZREMBYRANK(Type.ZSET),
    ZDIFFSTORE(Type.ZSET),
    ZINTERSTORE(Type.ZSET),
    ZUNIONSTORE(Type.ZSET),
    XADD(Type.STREAM),
    XTRIM(Type.STREAM),
    XDEL(Type.STREAM),
    XGROUP_CREATE("xgroup-create", Type.STREAM),
    XGROUP_CREATECONSUMER("xgroup-createconsumer", Type.STREAM),
    XGROUP_DELCONSUMER("xgroup-delconsumer", Type.STREAM),
    XGROUP_DESTROY("xgroup-destroy", Type.STREAM),
    XGROUP_SETID("xgroup-setid", Type.STREAM),
    XSETID(Type.STREAM),
    TS_ADD("ts.add", Type.TIMESERIES),
    JSON_SET("json.set", Type.JSON),
    PERSIST,
    EXPIRED,
    EVICTED,
    NEW_KEY("new", Type.NONE),
    UNKNOWN;

    private final String string;

    private final Type type;

    private KeyEvent() {
        this.string = this.name().toLowerCase();
        this.type = Type.NONE;
    }

    private KeyEvent(Type type) {
        this.string = this.name().toLowerCase();
        this.type = type;
    }

    private KeyEvent(String string, Type type) {
        this.string = string;
        this.type = type;
    }

    public String getString() {
        return string;
    }

    public Type getType() {
        return type;
    }

}
