package com.redis.spring.batch.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.timeseries.Sample;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class TsAdd<K, V, T> extends AbstractKeyOperation<K, V, T> {

    protected final Converter<T, Sample> sample;

    public TsAdd(Converter<T, K> key, Predicate<T> delete, Converter<T, Sample> sample) {
        super(key, delete);
        Assert.notNull(sample, "A sample converter is required");
        this.sample = sample;
    }

    @Override
    protected RedisFuture<?> doExecute(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        return commands.add(key, sample.convert(item));
    }

    public static <T> TsAddSampleBuilder<String, T> key(String key) {
        return new TsAddSampleBuilder<>(t -> key);
    }

    public static <K, T> TsAddSampleBuilder<K, T> key(K key) {
        return new TsAddSampleBuilder<>(t -> key);
    }

    public static <K, T> TsAddSampleBuilder<K, T> key(Converter<T, K> key) {
        return new TsAddSampleBuilder<>(key);
    }

    public static class TsAddSampleBuilder<K, T> {

        private final Converter<T, K> key;

        public TsAddSampleBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> TsAddBuilder<K, V, T> sample(Converter<T, Sample> sample) {
            return new TsAddBuilder<>(key, sample);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class TsAddBuilder<K, V, T> extends DelBuilder<K, V, T, Hset.HsetBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, Sample> sample;

        public TsAddBuilder(Converter<T, K> key, Converter<T, Sample> sample) {
            super(sample);
            this.key = key;
            this.sample = sample;
        }

        @Override
        public TsAdd<K, V, T> build() {
            return new TsAdd<>(key, del, sample);
        }
    }

}
