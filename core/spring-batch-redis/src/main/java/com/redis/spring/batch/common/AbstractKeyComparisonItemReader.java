package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.util.BatchUtils;

public abstract class AbstractKeyComparisonItemReader<T> extends AbstractItemStreamItemReader<KeyComparison<T>> {

    public static final int DEFAULT_CHUNK_SIZE = RedisItemReader.DEFAULT_CHUNK_SIZE;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private final RedisItemReader<String, String, T> source;

    private final RedisItemReader<String, String, T> target;

    private KeyComparator<T> comparator;

    private ScanKeyItemReader<String> keyReader;

    private SimpleOperationExecutor<String, String, String, T> sourceExecutor;

    private SimpleOperationExecutor<String, String, String, T> targetExecutor;

    private Iterator<KeyComparison<T>> iterator = Collections.emptyIterator();

    protected AbstractKeyComparisonItemReader(RedisItemReader<String, String, T> source,
            RedisItemReader<String, String, T> target) {
        setName(ClassUtils.getShortName(getClass()));
        this.source = source;
        this.target = target;
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
        comparator = comparator();
    }

    protected abstract KeyComparator<T> comparator();

    public boolean isOpen() {
        return BatchUtils.isOpen(sourceExecutor) && BatchUtils.isOpen(targetExecutor) && BatchUtils.isOpen(keyReader);
    }

    @Override
    public synchronized KeyComparison<T> read() throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        iterator = readNextChunk().iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    private List<KeyComparison<T>> readNextChunk() throws Exception {
        List<KeyComparison<T>> results = new ArrayList<>();
        List<String> keys = readKeys();
        List<T> sourceItems = sourceExecutor.process(keys);
        if (!CollectionUtils.isEmpty(sourceItems)) {
            List<T> targetItems = targetExecutor.process(keys);
            if (!CollectionUtils.isEmpty(targetItems)) {
                for (int index = 0; index < sourceItems.size(); index++) {
                    if (index >= targetItems.size()) {
                        continue;
                    }
                    T sourceItem = sourceItems.get(index);
                    T targetItem = targetItems.get(index);
                    Status status = comparator.compare(sourceItem, targetItem);
                    results.add(KeyComparison.source(sourceItem).target(targetItem).status(status).build());
                }
            }
        }
        return results;
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
