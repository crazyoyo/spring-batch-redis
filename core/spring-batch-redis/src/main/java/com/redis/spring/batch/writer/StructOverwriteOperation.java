package com.redis.spring.batch.writer;

import java.util.List;

import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.common.Struct.Type;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class StructOverwriteOperation<K, V> extends StructWriteOperation<K, V> {

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, Struct<K> item, List<RedisFuture<Object>> futures) {
        if (item.getType() != Type.STRING) {
            delete(commands, item, futures);
        }
        super.write(commands, item, futures);
    }

}
