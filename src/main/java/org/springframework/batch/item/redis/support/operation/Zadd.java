package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Zadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> score;

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

    public static <T> ZaddBuilder<T> builder() {
        return new ZaddBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class ZaddBuilder<T> extends CollectionOperationBuilder<T, ZaddBuilder<T>> {

        private Converter<T, Double> score;

        public Zadd<T> build() {
            return new Zadd<>(key, member, score);
        }

    }


}