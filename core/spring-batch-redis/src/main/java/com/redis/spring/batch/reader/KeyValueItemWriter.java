package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleOperationExecutor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueItemWriter<K, V, T> extends SimpleOperationExecutor<K, V, K, T> implements ItemStreamWriter<K> {

    private final BlockingQueue<T> queue;

    public KeyValueItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, K, T> operation,
            BlockingQueue<T> queue) {
        super(client, codec, operation);
        this.queue = queue;
    }

    @Override
    public void write(List<? extends K> items) throws InterruptedException {
        List<T> values = execute(items);
        for (T value : values) {
            try {
                queue.put(value);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }

}
