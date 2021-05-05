package org.springframework.batch.item.redis.support;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyComparisonResultCounter<K> implements KeyComparisonItemWriter.KeyComparisonResultHandler<K> {

    private final Map<KeyComparisonItemWriter.Result, AtomicLong> counters = Arrays.stream(KeyComparisonItemWriter.Result.values()).collect(Collectors.toMap(Function.identity(), r -> new AtomicLong()));

    @Override
    public void accept(DataStructure<K> source, DataStructure<K> target, KeyComparisonItemWriter.Result result) {
        counters.get(result).incrementAndGet();
    }

    public long get(KeyComparisonItemWriter.Result result) {
        return counters.get(result).get();
    }

    public boolean isOK() {
        return get(KeyComparisonItemWriter.Result.OK) > 0 && get(KeyComparisonItemWriter.Result.SOURCE) == 0 && get(KeyComparisonItemWriter.Result.TARGET) == 0 && get(KeyComparisonItemWriter.Result.TTL) == 0 && get(KeyComparisonItemWriter.Result.VALUE) == 0;
    }
}
