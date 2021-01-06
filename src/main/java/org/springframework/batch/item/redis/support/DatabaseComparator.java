package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Builder
public class DatabaseComparator<K> {

    public static Duration DEFAULT_TTL_TOLERANCE = Duration.ofSeconds(1);

    @NonNull
    private final ItemReader<DataStructure<K>> left;
    @NonNull
    private final ValueReader<K, DataStructure<K>> right;
    @Builder.Default
    private final Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
    @Builder.Default
    private final int chunkSize = KeyValueItemReaderBuilder.DEFAULT_CHUNK_SIZE;
    @Builder.Default
    private final int threads = KeyValueItemReaderBuilder.DEFAULT_THREAD_COUNT;

    public DatabaseComparison<K> execute() throws Exception {
        JobFactory factory = new JobFactory();
        factory.afterPropertiesSet();
        String name = ClassUtils.getShortName(getClass());
        KeyComparisonItemWriter<K> writer = new KeyComparisonItemWriter<>(right, ttlTolerance.getSeconds());
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(threads);
        TaskletStep step = factory.step(name + "-step").<DataStructure<K>, DataStructure<K>>chunk(chunkSize).reader(left).writer(writer).taskExecutor(taskExecutor).throttleLimit(threads).build();
        factory.getSyncLauncher().run(factory.job(name + "-job").start(step).build(), new JobParameters());
        return new DatabaseComparison<>(writer.getComparisons());
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
