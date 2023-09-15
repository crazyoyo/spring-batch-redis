package com.redis.spring.batch.reader;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.common.KeyValue;

public interface LuaToKeyValueFunction<T extends KeyValue<?, ?>> extends Function<List<Object>, T> {

    String getValueType();

}
