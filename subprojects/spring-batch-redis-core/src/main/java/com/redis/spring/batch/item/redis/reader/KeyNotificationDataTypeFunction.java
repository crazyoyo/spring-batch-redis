package com.redis.spring.batch.item.redis.reader;

import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.DataType;

public class KeyNotificationDataTypeFunction implements Function<String, DataType> {

	@Override
	public DataType apply(String event) {
		if (event == null) {
			return DataType.NONE;
		}
		String code = event.toLowerCase();
		if (code.startsWith("xgroup-")) {
			return DataType.STREAM;
		}
		if (code.startsWith("ts.")) {
			return DataType.TIMESERIES;
		}
		if (code.startsWith("json.")) {
			return DataType.JSON;
		}
		switch (code) {
		case "set":
		case "setrange":
		case "incrby":
		case "incrbyfloat":
		case "append":
			return DataType.STRING;
		case "lpush":
		case "rpush":
		case "rpop":
		case "lpop":
		case "linsert":
		case "lset":
		case "lrem":
		case "ltrim":
			return DataType.LIST;
		case "hset":
		case "hincrby":
		case "hincrbyfloat":
		case "hdel":
			return DataType.HASH;
		case "sadd":
		case "spop":
		case "sinterstore":
		case "sunionstore":
		case "sdiffstore":
			return DataType.SET;
		case "zincr":
		case "zadd":
		case "zrem":
		case "zrembyscore":
		case "zrembyrank":
		case "zdiffstore":
		case "zinterstore":
		case "zunionstore":
			return DataType.ZSET;
		case "xadd":
		case "xtrim":
		case "xdel":
		case "xsetid":
			return DataType.STREAM;
		default:
			return DataType.NONE;
		}
	}

}
