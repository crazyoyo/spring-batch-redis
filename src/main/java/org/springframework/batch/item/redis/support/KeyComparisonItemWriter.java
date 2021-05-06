package org.springframework.batch.item.redis.support;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {


    public enum Result {
        OK, SOURCE, TARGET, TYPE, TTL, VALUE
    }

    public static final Set<Result> MISMATCHES = new HashSet<>(Arrays.asList(Result.SOURCE, Result.TARGET, Result.TYPE, Result.TTL, Result.VALUE));

    public interface KeyComparisonResultHandler<K> {

        void accept(DataStructure<K> source, DataStructure<K> target, Result result);

    }

    private final ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader;
    private final long ttlTolerance;
    private final List<KeyComparisonResultHandler<K>> resultHandlers;

    public KeyComparisonItemWriter(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader, Duration ttlTolerance, List<KeyComparisonResultHandler<K>> resultHandlers) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(valueReader, "A value reader is required");
        Assert.notNull(ttlTolerance, "TTL tolerance cannot be null");
        Assert.isTrue(!ttlTolerance.isNegative(), "TTL tolerance must be positive");
        Assert.notEmpty(resultHandlers, "At least one result handler is required");
        this.valueReader = valueReader;
        this.ttlTolerance = ttlTolerance.toMillis();
        this.resultHandlers = resultHandlers;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        if (valueReader instanceof ItemStream) {
            ((ItemStream) valueReader).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (valueReader instanceof ItemStream) {
            ((ItemStream) valueReader).update(executionContext);
        }
        super.update(executionContext);
    }

    @Override
    public void close() {
        super.close();
        if (valueReader instanceof ItemStream) {
            ((ItemStream) valueReader).close();
        }
    }

    @Override
    public void write(List<? extends DataStructure<K>> sourceItems) throws Exception {
        List<DataStructure<K>> targetItems = valueReader.process(sourceItems.stream().map(DataStructure::getKey).collect(Collectors.toList()));
        if (targetItems == null || targetItems.size() != sourceItems.size()) {
            log.warn("Missing values in value reader response");
            return;
        }
        for (int index = 0; index < sourceItems.size(); index++) {
            DataStructure<K> source = sourceItems.get(index);
            DataStructure<K> target = targetItems.get(index);
            Result result = compare(source, target);
            for (KeyComparisonResultHandler<K> handler : resultHandlers) {
                handler.accept(source, target, result);
            }
        }
    }

    private Result compare(DataStructure<K> source, DataStructure<K> target) {
        if (DataStructure.NONE.equalsIgnoreCase(source.getType())) {
            if (DataStructure.NONE.equalsIgnoreCase(target.getType())) {
                return Result.OK;
            }
            return Result.TARGET;
        }
        if (DataStructure.NONE.equalsIgnoreCase(target.getType())) {
            return Result.SOURCE;
        }
        if (!ObjectUtils.nullSafeEquals(source.getType(), target.getType())) {
            return Result.TYPE;
        }
        if (source.getValue() == null) {
            if (target.getValue() == null) {
                return Result.OK;
            }
            return Result.TARGET;
        }
        if (target.getValue() == null) {
            return Result.SOURCE;
        }
        if (Objects.deepEquals(source.getValue(), target.getValue())) {
            if (Math.abs(source.getAbsoluteTTL() - target.getAbsoluteTTL()) > ttlTolerance) {
                return Result.TTL;
            }
            return Result.OK;
        }
        return Result.VALUE;
    }

    public static KeyComparisonItemWriterBuilder valueReader(ItemProcessor<List<? extends String>, List<DataStructure<String>>> valueReader) {
        return new KeyComparisonItemWriterBuilder(valueReader);
    }

    @Setter
    @Accessors(fluent = true)
    public static class KeyComparisonItemWriterBuilder {

        private static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

        private final ItemProcessor<List<? extends String>, List<DataStructure<String>>> valueReader;
        private final List<KeyComparisonResultHandler<String>> resultHandlers = new ArrayList<>();
        private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

        public KeyComparisonItemWriterBuilder(ItemProcessor<List<? extends String>, List<DataStructure<String>>> valueReader) {
            this.valueReader = valueReader;
        }

        public KeyComparisonItemWriterBuilder resultHandler(KeyComparisonResultHandler<String> resultHandler) {
            resultHandlers.add(resultHandler);
            return this;
        }

        public KeyComparisonItemWriter<String> build() {
            return new KeyComparisonItemWriter<>(valueReader, ttlTolerance, resultHandlers);
        }
    }

}