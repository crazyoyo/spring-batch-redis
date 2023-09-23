package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.DataStructureType;

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
    SET(DataStructureType.STRING),
    SETRANGE(DataStructureType.STRING),
    INCRBY(DataStructureType.STRING),
    INCRBYFLOAT(DataStructureType.STRING),
    APPEND(DataStructureType.STRING),
    LPUSH(DataStructureType.LIST),
    RPUSH(DataStructureType.LIST),
    RPOP(DataStructureType.LIST),
    LPOP(DataStructureType.LIST),
    LINSERT(DataStructureType.LIST),
    LSET(DataStructureType.LIST),
    LREM(DataStructureType.LIST),
    LTRIM(DataStructureType.LIST),
    HSET(DataStructureType.HASH),
    HINCRBY(DataStructureType.HASH),
    HINCRBYFLOAT(DataStructureType.HASH),
    HDEL(DataStructureType.HASH),
    SADD(DataStructureType.SET),
    SPOP(DataStructureType.SET),
    SINTERSTORE(DataStructureType.SET),
    SUNIONSTORE(DataStructureType.SET),
    SDIFFSTORE(DataStructureType.SET),
    ZINCR(DataStructureType.ZSET),
    ZADD(DataStructureType.ZSET),
    ZREM(DataStructureType.ZSET),
    ZREMBYSCORE(DataStructureType.ZSET),
    ZREMBYRANK(DataStructureType.ZSET),
    ZDIFFSTORE(DataStructureType.ZSET),
    ZINTERSTORE(DataStructureType.ZSET),
    ZUNIONSTORE(DataStructureType.ZSET),
    XADD(DataStructureType.STREAM),
    XTRIM(DataStructureType.STREAM),
    XDEL(DataStructureType.STREAM),
    XGROUP_CREATE("xgroup-create", DataStructureType.STREAM),
    XGROUP_CREATECONSUMER("xgroup-createconsumer", DataStructureType.STREAM),
    XGROUP_DELCONSUMER("xgroup-delconsumer", DataStructureType.STREAM),
    XGROUP_DESTROY("xgroup-destroy", DataStructureType.STREAM),
    XGROUP_SETID("xgroup-setid", DataStructureType.STREAM),
    XSETID(DataStructureType.STREAM),
    TS_ADD("ts.add", DataStructureType.TIMESERIES),
    JSON_SET("json.set", DataStructureType.JSON),
    PERSIST,
    EXPIRED,
    EVICTED,
    NEW_KEY("new", DataStructureType.NONE),
    UNKNOWN;

    private final String string;

    private final DataStructureType type;

    private KeyEvent() {
        this.string = this.name().toLowerCase();
        this.type = DataStructureType.NONE;
    }

    private KeyEvent(DataStructureType type) {
        this.string = this.name().toLowerCase();
        this.type = type;
    }

    private KeyEvent(String string, DataStructureType type) {
        this.string = string;
        this.type = type;
    }

    public String getString() {
        return string;
    }

    public DataStructureType getType() {
        return type;
    }

}
