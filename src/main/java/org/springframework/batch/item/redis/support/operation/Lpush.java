package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import org.springframework.core.convert.converter.Converter;

public class Lpush<T> extends AbstractCollectionOperation<T> {

    public Lpush(Converter<T, String> key, Converter<T, String> member) {
        super(key, member);
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisListAsyncCommands<String, String>) commands).lpush(key.convert(item), member.convert(item));
    }

    public static <T> LpushBuilder<T> builder() {
        return new LpushBuilder<>();
    }

    public static class LpushBuilder<T> extends CollectionOperationBuilder<T, LpushBuilder<T>> {

        public Lpush<T> build() {
            return new Lpush<>(key, member);
        }

    }

}