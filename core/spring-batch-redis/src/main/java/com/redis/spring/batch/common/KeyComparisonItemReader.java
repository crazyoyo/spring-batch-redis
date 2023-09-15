package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.Struct.Type;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.api.StatefulConnection;

public class KeyComparisonItemReader extends AbstractItemStreamItemReader<KeyComparison> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private final RedisItemReader<String, String, Struct<String>> left;

    private final RedisItemReader<String, String, Struct<String>> right;

    private GenericObjectPool<StatefulConnection<String, String>> rightPool;

    private Iterator<KeyComparison> iterator = Collections.emptyIterator();

    private Function<List<? extends String>, List<Struct<String>>> rightFunction;

    public KeyComparisonItemReader(RedisItemReader<String, String, Struct<String>> left,
            RedisItemReader<String, String, Struct<String>> right) {
        this.left = left;
        this.right = right;
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        if (left instanceof ItemStreamSupport) {
            ((ItemStreamSupport) left).setName(name + "-left");
        }
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
            rightPool = right.pool();
            rightFunction = right.toKeyValueFunction(rightPool);
            left.open(executionContext);
        }
    }

    public boolean isOpen() {
        return BatchUtils.isOpen(left);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);
        left.update(executionContext);
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            rightPool.close();
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
        List<Struct<String>> leftItems = left.readChunk();
        List<String> keys = leftItems.stream().map(Struct::getKey).collect(Collectors.toList());
        List<Struct<String>> rightItems = rightFunction.apply(keys);
        List<KeyComparison> results = new ArrayList<>();
        for (int index = 0; index < leftItems.size(); index++) {
            Struct<String> leftItem = leftItems.get(index);
            Struct<String> rightItem = getElement(rightItems, index);
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

    private Status compare(Struct<String> left, Struct<String> right) {
        if (right == null) {
            return Status.MISSING;
        }
        if (!Objects.equals(left.getType(), right.getType())) {
            if (right.getType() == Type.NONE) {
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
