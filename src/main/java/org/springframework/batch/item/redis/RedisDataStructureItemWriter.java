package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class RedisDataStructureItemWriter extends AbstractRedisItemWriter<DataStructure> {

    public RedisDataStructureItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout) {
	super(pool, commands, commandTimeout);
    }

    @SuppressWarnings({ "unchecked", "incomplete-switch" })
    @Override
    protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<String, String> commands,
	    List<? extends DataStructure> items) {
	List<RedisFuture<?>> futures = new ArrayList<>();
	for (DataStructure item : items) {
	    if (item.getValue() == null || item.noKeyTtl()) {
		futures.add(((RedisKeyAsyncCommands<String, String>) commands).del(item.getKey()));
		continue;
	    }
	    if (item.getValue() != null) {
		switch (item.getType()) {
		case STRING:
		    futures.add(((RedisStringAsyncCommands<String, String>) commands).set(item.getKey(),
			    (String) item.getValue()));
		    break;
		case LIST:
		    futures.add(((RedisListAsyncCommands<String, String>) commands).lpush(item.getKey(),
			    ((Collection<String>) item.getValue()).toArray(new String[0])));
		    break;
		case SET:
		    futures.add(((RedisSetAsyncCommands<String, String>) commands).sadd(item.getKey(),
			    ((Collection<String>) item.getValue()).toArray(new String[0])));
		    break;
		case ZSET:
		    ScoredValue<String>[] scoredValuesArray = ((Collection<ScoredValue<String>>) item.getValue())
			    .toArray(new ScoredValue[0]);
		    futures.add(((RedisSortedSetAsyncCommands<String, String>) commands).zadd(item.getKey(),
			    scoredValuesArray));
		    break;
		case HASH:
		    futures.add(((RedisHashAsyncCommands<String, String>) commands).hmset(item.getKey(),
			    (Map<String, String>) item.getValue()));
		    break;
		case STREAM:
		    Collection<StreamMessage<String, String>> messages = (Collection<StreamMessage<String, String>>) item
			    .getValue();
		    for (StreamMessage<String, String> message : messages) {
			futures.add(((RedisStreamAsyncCommands<String, String>) commands).xadd(item.getKey(),
				new XAddArgs().id(message.getId()), message.getBody()));
		    }
		    break;
		}
	    }
	    if (item.hasTtl()) {
		futures.add(((RedisKeyAsyncCommands<String, String>) commands).expire(item.getKey(), item.getTtl()));
	    }
	}
	return futures;
    }

    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, DataStructure item) {
	// not used
	return null;
    }

    public static RedisDataStructureItemWriterBuilder builder() {
	return new RedisDataStructureItemWriterBuilder();
    }

    public static class RedisDataStructureItemWriterBuilder
	    extends RedisConnectionBuilder<RedisDataStructureItemWriterBuilder> {

	public RedisDataStructureItemWriter build() {
	    return new RedisDataStructureItemWriter(pool(), async(), timeout());
	}

    }

}
