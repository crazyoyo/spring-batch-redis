package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import org.springframework.core.convert.converter.Converter;

public class Rpush<T> extends AbstractCollectionOperation<T> {

    public Rpush(Converter<T, String> key, Converter<T, String> member) {
        super(key, member);
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisListAsyncCommands<String, String>) commands).rpush(key.convert(item), member.convert(item));
    }

    public static <T> RpushBuilder<T> builder() {
        return new RpushBuilder<>();
    }

    public static class RpushBuilder<T> extends CollectionOperationBuilder<T, RpushBuilder<T>> {

        public Rpush<T> build() {
            return new Rpush<>(key, member);
        }

    }

}