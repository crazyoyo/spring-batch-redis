package com.redis.spring.batch.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Mode;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.util.KeyComparison.Status;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends AbstractItemStreamItemReader<KeyComparison> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private final RedisItemReader<String, String> left;

    private final RedisItemReader<String, String> right;

    private Iterator<KeyComparison> iterator = Collections.emptyIterator();

    private KeyValueItemProcessor<String, String> rightKeyValueReader;

    public KeyComparisonItemReader(AbstractRedisClient left, AbstractRedisClient right) {
        this(new RedisItemReader<>(left, StringCodec.UTF8), new RedisItemReader<>(right, StringCodec.UTF8));
    }

    public KeyComparisonItemReader(RedisItemReader<String, String> left, RedisItemReader<String, String> right) {
        this.left = left;
        this.right = right;
        setName(ClassUtils.getShortName(getClass()));
    }

    public RedisItemReader<String, String> getLeft() {
        return left;
    }

    public RedisItemReader<String, String> getRight() {
        return right;
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        left.setName(name + "-left");
        right.setName(name + "-right");
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        Assert.notNull(ttlTolerance, "Tolerance must not be null");
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (!isOpen()) {
            left.setValueType(ValueType.STRUCT);
            left.setMode(Mode.SCAN);
            right.setValueType(ValueType.STRUCT);
            right.setMode(Mode.SCAN);
            rightKeyValueReader = right.keyValueProcessor();
            rightKeyValueReader.open(executionContext);
            left.open(executionContext);
        }
    }

    public boolean isOpen() {
        return rightKeyValueReader != null && BatchUtils.isOpen(rightKeyValueReader) && left.isOpen();
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);
        left.update(executionContext);
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            left.close();
        }
        super.close();
    }

    @Override
    public synchronized KeyComparison read() throws Exception {
        if (!isOpen()) {
            return null;
        }
        if (iterator.hasNext()) {
            return iterator.next();
        }
        List<KeyValue<String>> leftItems = left.readChunk();
        List<String> keys = leftItems.stream().map(KeyValue::getKey).collect(Collectors.toList());
        List<KeyValue<String>> rightItems = rightKeyValueReader.process(keys);
        List<KeyComparison> results = new ArrayList<>();
        for (int index = 0; index < leftItems.size(); index++) {
            KeyValue<String> leftItem = leftItems.get(index);
            KeyValue<String> rightItem = getElement(rightItems, index);
            Status status = compare(leftItem, rightItem);
            results.add(new KeyComparison(leftItem, rightItem, status));
        }
        iterator = results.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    private <T> T getElement(List<T> list, int index) {
        if (list == null || index >= list.size()) {
            return null;
        }
        return list.get(index);
    }

    private Status compare(KeyValue<String> left, KeyValue<String> right) {
        if (right == null) {
            return Status.MISSING;
        }
        if (!Objects.equals(left.getType(), right.getType())) {
            if (KeyValue.isNone(right)) {
                return Status.MISSING;
            }
            return Status.TYPE;
        }
        if (!Objects.deepEquals(left.getValue(), right.getValue())) {
            return Status.VALUE;
        }
        Duration ttlDiff = Duration.ofMillis(Math.abs(left.getTtl() - right.getTtl()));
        if (ttlDiff.compareTo(ttlTolerance) > 0) {
            return Status.TTL;
        }
        return Status.OK;

    }

}
