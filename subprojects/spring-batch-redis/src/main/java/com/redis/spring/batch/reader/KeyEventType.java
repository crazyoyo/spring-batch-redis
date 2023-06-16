package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.DataStructure;

public enum KeyEventType {

	DEL, RENAME_FROM, RENAME_TO, MOVE_FROM, MOVE_TO, COPY_TO, RESTORE, EXPIRE, SORTSTORE, SET(DataStructure.STRING),
	SETRANGE(DataStructure.STRING), INCRBY(DataStructure.STRING), INCRBYFLOAT(DataStructure.STRING),
	APPEND(DataStructure.STRING), LPUSH(DataStructure.LIST), RPUSH(DataStructure.LIST), RPOP(DataStructure.LIST),
	LPOP(DataStructure.LIST), LINSERT(DataStructure.LIST), LSET(DataStructure.LIST), LREM(DataStructure.LIST),
	LTRIM(DataStructure.LIST), HSET(DataStructure.HASH), HINCRBY(DataStructure.HASH), HINCRBYFLOAT(DataStructure.HASH),
	HDEL(DataStructure.HASH), SADD(DataStructure.SET), SPOP(DataStructure.SET), SINTERSTORE(DataStructure.SET),
	SUNIONSTORE(DataStructure.SET), SDIFFSTORE(DataStructure.SET), ZINCR(DataStructure.ZSET), ZADD(DataStructure.ZSET),
	ZREM(DataStructure.ZSET), ZREMBYSCORE(DataStructure.ZSET), ZREMBYRANK(DataStructure.ZSET),
	ZDIFFSTORE(DataStructure.ZSET), ZINTERSTORE(DataStructure.ZSET), ZUNIONSTORE(DataStructure.ZSET),
	XADD(DataStructure.STREAM), XTRIM(DataStructure.STREAM), XDEL(DataStructure.STREAM),
	XGROUP_CREATE("xgroup-create", DataStructure.STREAM),
	XGROUP_CREATECONSUMER("xgroup-createconsumer", DataStructure.STREAM),
	XGROUP_DELCONSUMER("xgroup-delconsumer", DataStructure.STREAM),
	XGROUP_DESTROY("xgroup-destroy", DataStructure.STREAM), XGROUP_SETID("xgroup-setid", DataStructure.STREAM),
	XSETID(DataStructure.STREAM), TS_ADD("ts.add", DataStructure.TIMESERIES), JSON_SET("json.set", DataStructure.JSON),
	PERSIST, EXPIRED, EVICTED, NEW_KEY("new"), UNKNOWN;

	private final String string;
	private final String type;

	private KeyEventType() {
		this.string = this.name().toLowerCase();
		this.type = DataStructure.NONE;
	}

	private KeyEventType(String type) {
		this.string = this.name().toLowerCase();
		this.type = type;
	}

	private KeyEventType(String string, String type) {
		this.string = string;
		this.type = type;
	}

	public String getString() {
		return string;
	}

	public String getType() {
		return type;
	}

}
