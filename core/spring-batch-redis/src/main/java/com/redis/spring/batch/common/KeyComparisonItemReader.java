package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.util.BatchUtils;

public class KeyComparisonItemReader extends AbstractItemStreamItemReader<KeyComparison> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    public static final int DEFAULT_CHUNK_SIZE = RedisItemReader.DEFAULT_CHUNK_SIZE;

    private long ttlTolerance = DEFAULT_TTL_TOLERANCE.toMillis();

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private final RedisItemReader<String, String, KeyValue<String>> source;

    private final RedisItemReader<String, String, KeyValue<String>> target;

    private ScanKeyItemReader<String> keyReader;

    private SimpleOperationExecutor<String, String, String, KeyValue<String>> sourceExecutor;

    private SimpleOperationExecutor<String, String, String, KeyValue<String>> targetExecutor;

    private Iterator<KeyComparison> iterator = Collections.emptyIterator();

    public KeyComparisonItemReader(RedisItemReader<String, String, KeyValue<String>> source,
            RedisItemReader<String, String, KeyValue<String>> target) {
        this.source = source;
        this.target = target;
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    public void setName(String name) {
        source.setName(name + "-source");
        target.setName(name + "-target");
        super.setName(name);
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance.toMillis();
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        keyReader = source.scanKeyReader();
        targetExecutor = target.operationExecutor();
        targetExecutor.open(executionContext);
        sourceExecutor = source.operationExecutor();
        sourceExecutor.open(executionContext);
        keyReader.open(executionContext);
    }

    public boolean isOpen() {
        return BatchUtils.isOpen(sourceExecutor) && BatchUtils.isOpen(targetExecutor) && BatchUtils.isOpen(keyReader);
    }

    @Override
    public synchronized KeyComparison read() throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        iterator = readNextChunk().iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    private List<KeyComparison> readNextChunk() throws Exception {
        List<KeyComparison> results = new ArrayList<>();
        List<String> keys = readKeys();
        List<KeyValue<String>> sourceItems = sourceExecutor.process(keys);
        if (!CollectionUtils.isEmpty(sourceItems)) {
            List<KeyValue<String>> targetItems = targetExecutor.process(keys);
            if (!CollectionUtils.isEmpty(targetItems)) {
                for (int index = 0; index < sourceItems.size(); index++) {
                    if (index >= targetItems.size()) {
                        continue;
                    }
                    KeyValue<String> sourceItem = sourceItems.get(index);
                    KeyValue<String> targetItem = targetItems.get(index);
                    Status status = compare(sourceItem, targetItem);
                    KeyComparison comparison = new KeyComparison();
                    comparison.setSource(sourceItem);
                    comparison.setTarget(targetItem);
                    comparison.setStatus(status);
                    results.add(comparison);
                }
            }
        }
        return results;
    }

    private Status compare(KeyValue<String> source, KeyValue<String> target) {
        if (!target.exists() && source.exists()) {
            return Status.MISSING;
        }
        if (target.getType() != source.getType()) {
            return Status.TYPE;
        }
        if (!Objects.deepEquals(source.getValue(), target.getValue())) {
            return Status.VALUE;
        }
        if (source.getTtl() != target.getTtl()) {
            long delta = Math.abs(source.getTtl() - target.getTtl());
            if (delta > ttlTolerance) {
                return Status.TTL;
            }
        }
        return Status.OK;
    }

    private List<String> readKeys() throws Exception {
        List<String> keys = new ArrayList<>();
        String key;
        while (keys.size() < chunkSize && (key = keyReader.read()) != null) {
            keys.add(key);
        }
        return keys;
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);
        targetExecutor.update(executionContext);
        sourceExecutor.update(executionContext);
        keyReader.update(executionContext);
    }

    @Override
    public synchronized void close() {
        targetExecutor.close();
        sourceExecutor.close();
        keyReader.close();
        super.close();
    }

}
