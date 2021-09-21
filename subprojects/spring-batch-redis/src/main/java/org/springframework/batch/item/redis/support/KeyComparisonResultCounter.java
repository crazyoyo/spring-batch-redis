package org.springframework.batch.item.redis.support;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyComparisonResultCounter implements KeyComparisonItemWriter.KeyComparisonResultHandler {

    private final Map<KeyComparisonItemWriter.Status, AtomicLong> counters = Arrays.stream(KeyComparisonItemWriter.Status.values()).collect(Collectors.toMap(Function.identity(), r -> new AtomicLong()));

    @Override
    public void accept(DataStructure source, DataStructure target, KeyComparisonItemWriter.Status status) {
        counters.get(status).incrementAndGet();
    }

    public long get(KeyComparisonItemWriter.Status status) {
        return counters.get(status).get();
    }

    public Long[] get(KeyComparisonItemWriter.Status... statuses) {
        Long[] counts = new Long[statuses.length];
        for (int index = 0; index < statuses.length; index++) {
            counts[index] = get(statuses[index]);
        }
        return counts;
    }

    public boolean isOK() {
        for (KeyComparisonItemWriter.Status status : KeyComparisonItemWriter.MISMATCHES) {
            if (get(status) > 0) {
                return false;
            }
        }
        return true;
    }

}
