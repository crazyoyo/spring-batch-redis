package com.redis.spring.batch.reader;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.common.AbstractOperationItemStreamSupport;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemProcessor<K, V, I, O> extends AbstractOperationItemStreamSupport<K, V, I, O>
        implements ItemProcessor<List<I>, List<O>> {

    private Operation<K, V, I, O> operation;

    protected OperationItemProcessor(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, I, O> operation) {
        super(client, codec);
        this.operation = operation;
    }

    @Override
    protected Operation<K, V, I, O> operation() {
        return operation;
    }

    @Override
    public List<O> process(List<I> items) throws Exception {
        return execute(items);
    }

}
