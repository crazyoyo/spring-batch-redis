package com.redis.spring.batch.writer;

import com.redis.spring.batch.writer.operation.StructWrite;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemWriter<K, V> extends KeyValueItemWriter<K, V> {

    private boolean merge;

    public StructItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    @Override
    protected StructWrite<K, V> writeOperation() {
        StructWrite<K, V> operation = new StructWrite<>();
        operation.setOverwrite(!merge);
        return operation;
    }

}
