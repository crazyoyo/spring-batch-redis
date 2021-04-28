package org.springframework.batch.item.redis.support.operation;

import org.springframework.batch.item.redis.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public abstract class AbstractKeyOperation<T> implements RedisOperation<String, String, T> {

    protected final Converter<T, String> key;

    protected AbstractKeyOperation(Converter<T, String> key) {
        Assert.notNull(key, "A key converter is required");
        this.key = key;
    }

    public static class KeyOperationBuilder<T, B extends KeyOperationBuilder<T, B>> {

        protected Converter<T, String> key;

        public B key(Converter<T, String> key) {
            this.key = key;
            return (B) this;
        }

    }

}