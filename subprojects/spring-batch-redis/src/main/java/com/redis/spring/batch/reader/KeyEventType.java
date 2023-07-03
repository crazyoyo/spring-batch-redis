package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;

public enum KeyEventType {

	DEL, RENAME_FROM, RENAME_TO, MOVE_FROM, MOVE_TO, COPY_TO, RESTORE, EXPIRE, SORTSTORE, SET(KeyValue.STRING),
	SETRANGE(KeyValue.STRING), INCRBY(KeyValue.STRING), INCRBYFLOAT(KeyValue.STRING), APPEND(KeyValue.STRING),
	LPUSH(KeyValue.LIST), RPUSH(KeyValue.LIST), RPOP(KeyValue.LIST), LPOP(KeyValue.LIST), LINSERT(KeyValue.LIST),
	LSET(KeyValue.LIST), LREM(KeyValue.LIST), LTRIM(KeyValue.LIST), HSET(KeyValue.HASH), HINCRBY(KeyValue.HASH),
	HINCRBYFLOAT(KeyValue.HASH), HDEL(KeyValue.HASH), SADD(KeyValue.SET), SPOP(KeyValue.SET), SINTERSTORE(KeyValue.SET),
	SUNIONSTORE(KeyValue.SET), SDIFFSTORE(KeyValue.SET), ZINCR(KeyValue.ZSET), ZADD(KeyValue.ZSET), ZREM(KeyValue.ZSET),
	ZREMBYSCORE(KeyValue.ZSET), ZREMBYRANK(KeyValue.ZSET), ZDIFFSTORE(KeyValue.ZSET), ZINTERSTORE(KeyValue.ZSET),
	ZUNIONSTORE(KeyValue.ZSET), XADD(KeyValue.STREAM), XTRIM(KeyValue.STREAM), XDEL(KeyValue.STREAM),
	XGROUP_CREATE("xgroup-create", KeyValue.STREAM), XGROUP_CREATECONSUMER("xgroup-createconsumer", KeyValue.STREAM),
	XGROUP_DELCONSUMER("xgroup-delconsumer", KeyValue.STREAM), XGROUP_DESTROY("xgroup-destroy", KeyValue.STREAM),
	XGROUP_SETID("xgroup-setid", KeyValue.STREAM), XSETID(KeyValue.STREAM), TS_ADD("ts.add", KeyValue.TIMESERIES),
	JSON_SET("json.set", KeyValue.JSON), PERSIST, EXPIRED, EVICTED, NEW_KEY("new"), UNKNOWN;

	private final String string;
	private final String type;

	private KeyEventType() {
		this.string = this.name().toLowerCase();
		this.type = KeyValue.NONE;
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
