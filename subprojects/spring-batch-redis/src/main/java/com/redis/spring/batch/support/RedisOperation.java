package com.redis.spring.batch.support;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public interface RedisOperation<K, V, T> {

    RedisFuture<?> execute(RedisModulesAsyncCommands<K, V> commands, T item);

    interface RedisOperationBuilder<T> {

        RedisOperation<String, String, T> build();

    }

    class NullValuePredicate<T> implements Predicate<T> {

        private final Converter<T, ?> value;

        public NullValuePredicate(Converter<T, ?> value) {
            Assert.notNull(value, "A value converter is required");
            this.value = value;
        }

        @Override
        public boolean test(T t) {
            return value.convert(t) == null;
        }

    }

    abstract class DelBuilder<T, B extends DelBuilder<T, B>> implements RedisOperationBuilder<T> {

        protected Predicate<T> del;

        protected DelBuilder(Converter<T, ?> value) {
            this.del = new NullValuePredicate<>(value);
        }

        @SuppressWarnings("unchecked")
        public B del(Predicate<T> del) {
            this.del = del;
            return (B) this;
        }

    }

    abstract class RemoveBuilder<T, B extends RemoveBuilder<T, B>> extends DelBuilder<T, B> {

        protected Predicate<T> remove = t -> false;

        public RemoveBuilder(Converter<T, ?> value) {
            super(value);
        }

        @SuppressWarnings("unchecked")
        public B remove(Predicate<T> remove) {
            this.remove = remove;
            return (B) this;
        }

    }

}