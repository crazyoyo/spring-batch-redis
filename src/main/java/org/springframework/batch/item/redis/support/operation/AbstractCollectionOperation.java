package org.springframework.batch.item.redis.support.operation;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public abstract class AbstractCollectionOperation<T> extends AbstractKeyOperation<T> {

    protected final Converter<T, String> member;

    protected AbstractCollectionOperation(Converter<T, String> key, Converter<T, String> member) {
        super(key);
        Assert.notNull(member, "A member id converter is required");
        this.member = member;
    }


    public static class CollectionOperationBuilder<T, B extends CollectionOperationBuilder<T, B>> extends KeyOperationBuilder<T, B> {

        protected Converter<T, String> member;

        public B member(Converter<T, String> member) {
            this.member = member;
            return (B) this;
        }

    }


}