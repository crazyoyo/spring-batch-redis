package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import java.util.*;
import java.util.stream.Collectors;

@Builder
public class DatabaseComparator<K, V> {

    @NonNull
    private final ItemReader<DataStructure<K>> left;
    @NonNull
    private final ValueReader<K, DataStructure<K>> right;
    @Builder.Default
    private final ComparatorOptions options = ComparatorOptions.builder().build();

    public DatabaseComparison execute() throws Exception {
        JobFactory<DataStructure<K>, DataStructure<K>> factory = new JobFactory<>();
        factory.afterPropertiesSet();
        String name = ClassUtils.getShortName(getClass());
        KeyComparisonItemWriter<K> writer = new KeyComparisonItemWriter<>(right, options.getTtlTolerance().getSeconds());
        TaskletStep step = factory.<DataStructure<K>, DataStructure<K>>step(name + "-step", options.getJobOptions()).reader(left).writer(writer).build();
        Job job = factory.getJobBuilderFactory().get(name + "-job").start(step).build();
        factory.execute(job, new JobParameters());
        return new DatabaseComparison(writer.getComparisons());
    }

    private static class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {

        @Getter
        private final Map<KeyComparison.Result, List<K>> comparisons;
        private final ValueReader<K, DataStructure<K>> right;
        private final long ttlTolerance;

        public KeyComparisonItemWriter(ValueReader<K, DataStructure<K>> right, long ttlTolerance) {
            this.right = right;
            comparisons = new HashMap<>();
            for (KeyComparison.Result status : KeyComparison.Result.values()) {
                comparisons.put(status, new ArrayList<>());
            }
            this.ttlTolerance = ttlTolerance;
        }

        @Override
        public void write(List<? extends DataStructure<K>> items) throws Exception {
            List<K> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
            List<DataStructure<K>> rightItems = right.values(keys);
            for (int index = 0; index < items.size(); index++) {
                DataStructure<K> left = items.get(index);
                DataStructure<K> right = rightItems.get(index);
                KeyComparison.Result result = compare(left, right);
                comparisons.get(result).add(left.getKey());
            }
        }

        private KeyComparison.Result compare(DataStructure<K> left, DataStructure<K> right) {
            if (left.getValue() == null) {
                if (right.getValue() == null) {
                    return KeyComparison.Result.OK;
                }
                return KeyComparison.Result.RIGHT_ONLY;
            }
            if (right.getValue() == null) {
                return KeyComparison.Result.LEFT_ONLY;
            }
            if (Objects.deepEquals(left.getValue(), right.getValue())) {
                long ttlDiff = Math.abs(left.getTtl() - right.getTtl());
                if (ttlDiff > ttlTolerance) {
                    return KeyComparison.Result.TTL;
                }
                return KeyComparison.Result.OK;
            }
            return KeyComparison.Result.VALUE;
        }
    }


}
