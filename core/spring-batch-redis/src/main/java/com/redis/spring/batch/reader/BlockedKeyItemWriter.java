package com.redis.spring.batch.reader;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemWriter;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class BlockedKeyItemWriter<K> implements ItemWriter<KeyValue<K>> {

    private final Set<String> blockedKeys;

    private final Function<K, String> toStringKeyFunction;

    private final Predicate<KeyValue<K>> predicate;

    public BlockedKeyItemWriter(RedisCodec<K, ?> codec, DataSize memoryUsageLimit, Set<String> blockedKeys) {
        this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
        this.predicate = new KeyMemoryUsagePredicate<>(memoryUsageLimit);
        this.blockedKeys = blockedKeys;
    }

    @Override
    public void write(List<? extends KeyValue<K>> items) throws Exception {
        items.stream().filter(predicate).map(KeyValue::getKey).map(toStringKeyFunction).forEach(blockedKeys::add);
    }

    private static class KeyMemoryUsagePredicate<K> implements Predicate<KeyValue<K>> {

        private final long limit;

        public KeyMemoryUsagePredicate(DataSize limit) {
            this.limit = limit.toBytes();
        }

        @Override
        public boolean test(KeyValue<K> t) {
            if (t == null) {
                return false;
            }
            return KeyValue.hasMemoryUsage(t) && t.getMemoryUsage() > limit;
        }

    }

}
