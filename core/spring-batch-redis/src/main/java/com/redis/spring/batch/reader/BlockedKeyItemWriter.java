package com.redis.spring.batch.reader;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemWriter;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class BlockedKeyItemWriter<K, T extends KeyValue<K, ?>> implements ItemWriter<T> {

    private final Set<String> blockedKeys;

    private final Function<K, String> toStringKeyFunction;

    private final Predicate<T> predicate;

    public BlockedKeyItemWriter(RedisCodec<K, ?> codec, DataSize memoryUsageLimit, Set<String> blockedKeys) {
        this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
        this.predicate = new BlockedKeyPredicate<>(memoryUsageLimit);
        this.blockedKeys = blockedKeys;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        items.stream().filter(predicate).map(KeyValue::getKey).map(toStringKeyFunction).forEach(blockedKeys::add);
    }

    private static class BlockedKeyPredicate<K, T extends KeyValue<K, ?>> implements Predicate<T> {

        private final DataSize limit;

        public BlockedKeyPredicate(DataSize limit) {
            this.limit = limit;
        }

        @Override
        public boolean test(T t) {
            if (t == null) {
                return false;
            }
            return t.getMemoryUsage() != null && t.getMemoryUsage().compareTo(limit) > 0;
        }

    }

}
