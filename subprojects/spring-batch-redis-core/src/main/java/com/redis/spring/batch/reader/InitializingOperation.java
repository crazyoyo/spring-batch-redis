package com.redis.spring.batch.reader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.Operation;

public interface InitializingOperation<K, V, I, O> extends Operation<K, V, I, O> {

	void afterPropertiesSet(StatefulRedisModulesConnection<K, V> connection) throws Exception;

}
