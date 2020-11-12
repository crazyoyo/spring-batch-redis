package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisDumpItemReader extends AbstractKeyValueItemReader<KeyValue<byte[]>> {

    public RedisDumpItemReader(ItemReader<String> keyReader, int threadCount, int batchSize, int queueCapacity,
	    long queuePollingTimeout, GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout) {
	super(keyReader, threadCount, batchSize, queueCapacity, queuePollingTimeout, pool, commands, commandTimeout);
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

    public static RedisDumpItemReaderBuilder builder() {
	return new RedisDumpItemReaderBuilder();
    }

    public static class RedisDumpItemReaderBuilder extends KeyValueItemReaderBuilder<RedisDumpItemReaderBuilder> {

	public RedisDumpItemReader build() {
	    return new RedisDumpItemReader(keyReader(), threadCount, batchSize, queueCapacity, queuePollingTimeout,
		    pool(), async(), timeout());
	}

    }

}
