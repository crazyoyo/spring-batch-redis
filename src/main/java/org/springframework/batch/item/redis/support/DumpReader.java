package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DumpReader extends AbstractValueReader<KeyValue<byte[]>> {

	public DumpReader(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) {
		super(client, poolConfig);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyValue<byte[]>> read(List<? extends String> keys, BaseRedisAsyncCommands<String, String> commands)
			throws Exception {
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
		for (String key : keys) {
			ttlFutures.add(((RedisKeyAsyncCommands<String, String>) commands).ttl(key));
			dumpFutures.add(((RedisKeyAsyncCommands<String, String>) commands).dump(key));
		}
		commands.flushCommands();
		List<KeyValue<byte[]>> dumps = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			KeyValue<byte[]> dump = new KeyValue<>();
			dump.setKey(keys.get(index));
			try {
				dump.setTtl(getTtl(ttlFutures.get(index)));
				dump.setValue(get(dumpFutures.get(index)));
			} catch (Exception e) {
				log.error("Could not get value", e);
			}
			dumps.add(dump);
		}
		return dumps;
	}

}
