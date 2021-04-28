package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.CommandBuilder;

import java.util.function.Function;
import java.util.function.Supplier;

public class KeyDumpItemWriter<K, V, T> extends OperationItemWriter<K, V, T> {

    public KeyDumpItemWriter(Supplier<StatefulConnection<K, V>> statefulConnectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(statefulConnectionSupplier, poolConfig, async, operation);
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

        public KeyDumpItemWriter<String, String, KeyValue<String, byte[]>> build() {
            return new KeyDumpItemWriter<>(connectionSupplier, poolConfig, async, RedisOperation.<KeyValue<String,byte[]>>restore().key(KeyValue::getKey).dump(KeyValue::getValue).absoluteTTL(KeyValue::getAbsoluteTTL).build());
        }

    }

}
