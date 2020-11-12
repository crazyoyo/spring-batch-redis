package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataType;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisDataStructureItemReader extends AbstractKeyValueItemReader<DataStructure> {

    public RedisDataStructureItemReader(ItemReader<String> keyReader, int threadCount, int batchSize, int queueCapacity,
	    long queuePollingTimeout, GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout) {
	super(keyReader, threadCount, batchSize, queueCapacity, queuePollingTimeout, pool, commands, commandTimeout);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<DataStructure> read(List<? extends String> keys, BaseRedisAsyncCommands<String, String> commands)
	    throws Exception {
	List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
	for (String key : keys) {
	    typeFutures.add(((RedisKeyAsyncCommands<String, String>) commands).type(key));
	}
	commands.flushCommands();
	List<DataStructure> values = new ArrayList<>(keys.size());
	List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
	List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
	for (int index = 0; index < keys.size(); index++) {
	    String key = keys.get(index);
	    String typeName;
	    try {
		typeName = get(typeFutures.get(index));
	    } catch (Exception e) {
		log.error("Could not get type", e);
		continue;
	    }
	    DataType type = DataType.fromCode(typeName);
	    valueFutures.add(getValue(commands, key, type));
	    ttlFutures.add(((RedisKeyAsyncCommands<String, String>) commands).ttl(key));
	    DataStructure dataStructure = new DataStructure();
	    dataStructure.setKey(key);
	    dataStructure.setType(type);
	    values.add(dataStructure);
	}
	commands.flushCommands();
	for (int index = 0; index < values.size(); index++) {
	    DataStructure dataStructure = values.get(index);
	    try {
		dataStructure.setValue(get(valueFutures.get(index)));
	    } catch (Exception e) {
		log.error("Could not get value", e);
	    }
	    try {
		dataStructure.setTtl(getTtl(ttlFutures.get(index)));
	    } catch (Exception e) {
		log.error("Could not get ttl", e);
	    }
	}
	return values;
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<?> getValue(BaseRedisAsyncCommands<String, String> commands, String key, DataType type) {
	if (type == null) {
	    return null;
	}
	switch (type) {
	case HASH:
	    return ((RedisHashAsyncCommands<String, String>) commands).hgetall(key);
	case LIST:
	    return ((RedisListAsyncCommands<String, String>) commands).lrange(key, 0, -1);
	case SET:
	    return ((RedisSetAsyncCommands<String, String>) commands).smembers(key);
	case STREAM:
	    return ((RedisStreamAsyncCommands<String, String>) commands).xrange(key, Range.create("-", "+"));
	case STRING:
	    return ((RedisStringAsyncCommands<String, String>) commands).get(key);
	case ZSET:
	    return ((RedisSortedSetAsyncCommands<String, String>) commands).zrangeWithScores(key, 0, -1);
	default:
	    return null;
	}
    }

    public static RedisDataStructureItemReaderBuilder builder() {
	return new RedisDataStructureItemReaderBuilder();
    }

    public static class RedisDataStructureItemReaderBuilder
	    extends KeyValueItemReaderBuilder<RedisDataStructureItemReaderBuilder> {

	public RedisDataStructureItemReader build() {
	    return new RedisDataStructureItemReader(keyReader(), threadCount, batchSize, queueCapacity,
		    queuePollingTimeout, pool(), async(), timeout());
	}

    }

}
