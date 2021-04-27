package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.RedisOperation;

import java.util.function.Function;
import java.util.function.Supplier;

public class KeyDumpItemWriter<K, V> extends OperationItemWriter<K, V, KeyValue<K, byte[]>> {

    public KeyDumpItemWriter(Supplier<StatefulConnection<K, V>> statefulConnectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
        super(statefulConnectionSupplier, poolConfig, async, new RedisOperation.RestoreOperation<>(KeyValue::getKey, KeyValue::getValue, KeyValue::getAbsoluteTTL, v -> true));
    }

    public static KeyDumpItemWriterBuilder client(RedisClient client) {
        return new KeyDumpItemWriterBuilder(client);
    }

    public static KeyDumpItemWriterBuilder client(RedisClusterClient client) {
        return new KeyDumpItemWriterBuilder(client);
    }

    public static class KeyDumpItemWriterBuilder extends CommandBuilder<DataStructureItemWriter.DataStructureItemWriterBuilder> {

        public KeyDumpItemWriterBuilder(RedisClusterClient client) {
            super(client);
        }

        public KeyDumpItemWriterBuilder(RedisClient client) {
            super(client);
        }

        public KeyDumpItemWriter<String, String> build() {
            return new KeyDumpItemWriter<>(connectionSupplier, poolConfig, async);
        }

    }

}
