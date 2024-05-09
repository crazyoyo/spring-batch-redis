package com.redis.spring.batch.item.redis.common;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum DataType {

	NONE("none"), HASH("hash"), JSON("ReJSON-RL"), LIST("list"), SET("set"), STREAM("stream"), STRING("string"),
	TIMESERIES("TSDB-TYPE"), ZSET("zset");

	private static final Map<String, DataType> TYPE_MAP = Stream.of(DataType.values())
			.collect(Collectors.toMap(t -> t.getString().toLowerCase(), Function.identity()));

	private final String string;

	private DataType(String string) {
		this.string = string;
	}

	public String getString() {
		return string;
	}

	public static DataType of(String string) {
		return TYPE_MAP.get(string.toLowerCase());
	}

}