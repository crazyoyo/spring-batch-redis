package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisSortedSetItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

    private final Converter<T, Double> scoreConverter;

    public RedisSortedSetItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
	    Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
	    long commandTimeout, Converter<T, String> keyConverter, Converter<T, String> memberIdConverter,
	    Converter<T, Double> scoreConverter) {
	super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
	Assert.notNull(scoreConverter, "A score converter is required.");
	this.scoreConverter = scoreConverter;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
	    T item) {
	Double score = scoreConverter.convert(item);
	if (score == null) {
	    return null;
	}
	return ((RedisSortedSetAsyncCommands<String, String>) commands).zadd(key, score, memberId);
    }

    public static <T> RedisSortedSetItemWriterBuilder<T> builder() {
	return new RedisSortedSetItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisSortedSetItemWriterBuilder<T>
	    extends AbstractCollectionCommandItemWriterBuilder<T, RedisSortedSetItemWriterBuilder<T>> {

	private Converter<T, Double> scoreConverter;

	public RedisSortedSetItemWriter<T> build() {
	    return new RedisSortedSetItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter,
		    scoreConverter);
	}

    }

}
