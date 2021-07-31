package org.springframework.batch.item.redis.support;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyComparisonResultCounter implements KeyComparisonItemWriter.KeyComparisonResultHandler {

    private final Map<KeyComparisonItemWriter.Result, AtomicLong> counters = Arrays.stream(KeyComparisonItemWriter.Result.values()).collect(Collectors.toMap(Function.identity(), r -> new AtomicLong()));

    @Override
    public void accept(DataStructure source, DataStructure target, KeyComparisonItemWriter.Result result) {
        counters.get(result).incrementAndGet();
    }

    public long get(KeyComparisonItemWriter.Result result) {
        return counters.get(result).get();
    }

    public Long[] get(KeyComparisonItemWriter.Result... results) {
        Long[] counts = new Long[results.length];
        for (int index = 0; index < results.length; index++) {
            counts[index] = get(results[index]);
        }
        return counts;
    }

    public boolean isOK() {
        if (get(KeyComparisonItemWriter.Result.OK) == 0) {
            return false;
        }
        for (KeyComparisonItemWriter.Result mismatch : KeyComparisonItemWriter.MISMATCHES) {
            if (get(mismatch) > 0) {
                return false;
            }
        }
        return true;
    }
}
