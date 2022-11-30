package com.redis.spring.batch.writer;

import java.util.Collection;
import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;

public interface PipelinedOperation<K, V, T> {

	@SuppressWarnings("rawtypes")
	Collection<RedisFuture> execute(StatefulConnection<K, V> connection, List<? extends T> items);

}