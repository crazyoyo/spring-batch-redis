package com.redis.spring.batch.common;

import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum DataType {

	HASH("hash"), JSON("ReJSON-RL"), LIST("list"), SET("set"), STREAM("stream"), STRING("string"),
	TIMESERIES("TSDB-TYPE"), ZSET("zset");

	private static final Function<DataType, String> DATATYPE_STRING = DataType::getString;

	private static final UnaryOperator<String> TO_LOWER_CASE = String::toLowerCase;

	private static final Map<String, DataType> TYPE_MAP = Stream.of(DataType.values())
			.collect(Collectors.toMap(DATATYPE_STRING.andThen(TO_LOWER_CASE), Function.identity()));

	private final String string;

	private DataType(String string) {
		this.string = string;
	}

	public String getString() {
		return string;
	}

	public static DataType of(String string) {
		return TYPE_MAP.get(TO_LOWER_CASE.apply(string));
	}

}
