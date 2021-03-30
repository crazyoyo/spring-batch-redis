package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyDumpOperation;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.OperationItemWriter;

public class RedisClusterKeyDumpItemWriter<K, V> extends OperationItemWriter<K, V, StatefulRedisClusterConnection<K, V>, KeyValue<K, byte[]>> {

    public RedisClusterKeyDumpItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool) {
        super(pool, StatefulRedisClusterConnection::async, new KeyDumpOperation<>());
    }

    public RedisClusterKeyDumpItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, boolean replace) {
        super(pool, StatefulRedisClusterConnection::async, new KeyDumpOperation<>(replace));
    }

}
