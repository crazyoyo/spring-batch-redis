package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.DataType;

public enum KeyEvent {

	DEL, RENAME_FROM, RENAME_TO, MOVE_FROM, MOVE_TO, COPY_TO, RESTORE, EXPIRE, SORTSTORE, SET(DataType.STRING),
	SETRANGE(DataType.STRING), INCRBY(DataType.STRING), INCRBYFLOAT(DataType.STRING), APPEND(DataType.STRING),
	LPUSH(DataType.LIST), RPUSH(DataType.LIST), RPOP(DataType.LIST), LPOP(DataType.LIST), LINSERT(DataType.LIST),
	LSET(DataType.LIST), LREM(DataType.LIST), LTRIM(DataType.LIST), HSET(DataType.HASH), HINCRBY(DataType.HASH),
	HINCRBYFLOAT(DataType.HASH), HDEL(DataType.HASH), SADD(DataType.SET), SPOP(DataType.SET), SINTERSTORE(DataType.SET),
	SUNIONSTORE(DataType.SET), SDIFFSTORE(DataType.SET), ZINCR(DataType.ZSET), ZADD(DataType.ZSET), ZREM(DataType.ZSET),
	ZREMBYSCORE(DataType.ZSET), ZREMBYRANK(DataType.ZSET), ZDIFFSTORE(DataType.ZSET), ZINTERSTORE(DataType.ZSET),
	ZUNIONSTORE(DataType.ZSET), XADD(DataType.STREAM), XTRIM(DataType.STREAM), XDEL(DataType.STREAM),
	XGROUP_CREATE("xgroup-create", DataType.STREAM), XGROUP_CREATECONSUMER("xgroup-createconsumer", DataType.STREAM),
	XGROUP_DELCONSUMER("xgroup-delconsumer", DataType.STREAM), XGROUP_DESTROY("xgroup-destroy", DataType.STREAM),
	XGROUP_SETID("xgroup-setid", DataType.STREAM), XSETID(DataType.STREAM), TS_ADD("ts.add", DataType.TIMESERIES),
	JSON_SET("json.set", DataType.JSON), PERSIST, EXPIRED, EVICTED, NEW_KEY("new", null), UNKNOWN;

	private final String string;

	private final DataType type;

	private KeyEvent() {
		this.string = this.name().toLowerCase();
		this.type = null;
	}

	private KeyEvent(DataType type) {
		this.string = this.name().toLowerCase();
		this.type = type;
	}

	private KeyEvent(String string, DataType type) {
		this.string = string;
		this.type = type;
	}

	public String getString() {
		return string;
	}

	public DataType getType() {
		return type;
	}

}
