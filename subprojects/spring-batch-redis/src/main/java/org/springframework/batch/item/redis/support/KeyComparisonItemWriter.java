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
public class KeyComparisonItemWriter extends AbstractItemStreamItemWriter<DataStructure> {

    public enum Status {
        OK, // No difference
        MISSING, // Key missing in target database
        TYPE, // Type mismatch
        TTL, // TTL mismatch
        VALUE // Value mismatch
    }

    public static final Set<Status> MISMATCHES = new HashSet<>(Arrays.asList(Status.MISSING, Status.TYPE, Status.TTL, Status.VALUE));

    public interface KeyComparisonResultHandler {

        void accept(DataStructure source, DataStructure target, Status status);

    }

    private final ItemProcessor<List<? extends String>, List<DataStructure>> valueReader;
    private final long ttlTolerance;
    private final List<KeyComparisonResultHandler> resultHandlers;

    public KeyComparisonItemWriter(ItemProcessor<List<? extends String>, List<DataStructure>> valueReader, Duration ttlTolerance, List<KeyComparisonResultHandler> resultHandlers) {
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
    public void write(List<? extends DataStructure> sourceItems) throws Exception {
        List<DataStructure> targetItems = valueReader.process(sourceItems.stream().map(DataStructure::getKey).collect(Collectors.toList()));
        if (targetItems == null || targetItems.size() != sourceItems.size()) {
            log.warn("Missing values in value reader response");
            return;
        }
        for (int index = 0; index < sourceItems.size(); index++) {
            DataStructure source = sourceItems.get(index);
            DataStructure target = targetItems.get(index);
            Status status = compare(source, target);
            for (KeyComparisonResultHandler handler : resultHandlers) {
                handler.accept(source, target, status);
            }
        }
    }

    private Status compare(DataStructure source, DataStructure target) {
        if (DataStructure.NONE.equalsIgnoreCase(source.getType())) {
            if (DataStructure.NONE.equalsIgnoreCase(target.getType())) {
                return Status.OK;
            }
            return Status.TYPE;
        }
        if (DataStructure.NONE.equalsIgnoreCase(target.getType())) {
            return Status.MISSING;
        }
        if (!ObjectUtils.nullSafeEquals(source.getType(), target.getType())) {
            return Status.TYPE;
        }
        if (source.getValue() == null) {
            if (target.getValue() == null) {
                return Status.OK;
            }
            return Status.VALUE;
        }
        if (target.getValue() == null) {
            return Status.MISSING;
        }
        if (Objects.deepEquals(source.getValue(), target.getValue())) {
            if (Math.abs(source.getAbsoluteTTL() - target.getAbsoluteTTL()) > ttlTolerance) {
                return Status.TTL;
            }
            return Status.OK;
        }
        return Status.VALUE;
    }

    public static KeyComparisonItemWriterBuilder valueReader(ItemProcessor<List<? extends String>, List<DataStructure>> valueReader) {
        return new KeyComparisonItemWriterBuilder(valueReader);
    }

    @Setter
    @Accessors(fluent = true)
    public static class KeyComparisonItemWriterBuilder {

        private static final Duration DEFAULT_TTL_TOLERANCE = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL.multipliedBy(2);

        private final ItemProcessor<List<? extends String>, List<DataStructure>> valueReader;
        private final List<KeyComparisonResultHandler> resultHandlers = new ArrayList<>();
        private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

        public KeyComparisonItemWriterBuilder(ItemProcessor<List<? extends String>, List<DataStructure>> valueReader) {
            this.valueReader = valueReader;
        }

        public KeyComparisonItemWriterBuilder resultHandler(KeyComparisonResultHandler resultHandler) {
            resultHandlers.add(resultHandler);
            return this;
        }

        public KeyComparisonItemWriterBuilder resultHandlers(KeyComparisonResultHandler... resultHandlers) {
            this.resultHandlers.addAll(Arrays.asList(resultHandlers));
            return this;
        }

        public KeyComparisonItemWriter build() {
            return new KeyComparisonItemWriter(valueReader, ttlTolerance, resultHandlers);
        }
    }

}
