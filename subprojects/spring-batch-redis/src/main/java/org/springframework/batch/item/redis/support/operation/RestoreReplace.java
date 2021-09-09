package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RestoreArgs;
import org.springframework.core.convert.converter.Converter;

public class RestoreReplace<K, V, T> extends Restore<K, V, T> {

    public RestoreReplace(Converter<T, K> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL) {
        super(key, value, absoluteTTL);
    }

    @Override
    protected RestoreArgs args(T item) {
        return super.args(item).replace();
    }
}
