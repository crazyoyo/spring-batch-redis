package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Builder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Zadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> score;

    @Builder
    public Zadd(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> score) {
        super(key, member);
        Assert.notNull(score, "A score converter is required");
        this.score = score;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        Double scoreValue = score.convert(item);
        if (scoreValue == null) {
            return null;
        }
        return ((RedisSortedSetAsyncCommands<String, String>) commands).zadd(key.convert(item), scoreValue, member.convert(item));
    }

}