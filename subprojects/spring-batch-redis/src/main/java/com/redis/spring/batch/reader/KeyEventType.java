package com.redis.spring.batch.reader;

public enum KeyEventType {

	DEL, RENAME_FROM, RENAME_TO, MOVE_FROM, MOVE_TO, COPY_TO, RESTORE, EXPIRE, SORTSTORE, SET, SETRANGE, INCRBY,
	INCRBYFLOAT, APPEND, LPUSH, RPUSH, RPOP, LPOP, LINSERT, LSET, LREM, LTRIM, HSET, HINCRBY, HINCRBYFLOAT, HDEL, SADD,
	SPOP, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, ZINCR, ZADD, ZREM, ZREMBYSCORE, ZREMBYRANK, ZDIFFSTORE, ZINTERSTORE,
	ZUNIONSTORE, XADD, XTRIM, XDEL, XGROUP_CREATE("xgroup-create"), XGROUP_CREATECONSUMER("xgroup-createconsumer"),
	XGROUP_DELCONSUMER("xgroup-delconsumer"), XGROUP_DESTROY("xgroup-destroy"), XGROUP_SETID("xgroup-setid"), XSETID,
	TS_ADD("ts.add"), JSON_SET("json.set"), PERSIST, EXPIRED, EVICTED, NEW_KEY("new"), UNKNOWN;

	private String string;

	private KeyEventType() {
		this.string = this.name().toLowerCase();
	}

	private KeyEventType(String string) {
		this.string = string;
	}

	public String getString() {
		return string;
	}

}
