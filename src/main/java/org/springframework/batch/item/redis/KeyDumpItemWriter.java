package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.operation.Restore;

import java.util.function.Function;
import java.util.function.Supplier;

public class KeyDumpItemWriter extends OperationItemWriter<KeyValue<byte[]>> {

    public KeyDumpItemWriter(Supplier<StatefulConnection<String, String>> statefulConnectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
        super(statefulConnectionSupplier, poolConfig, async, restoreOperation());
    }

    private static RedisOperation<KeyValue<byte[]>> restoreOperation() {
        return new Restore<>(KeyValue::getKey, KeyValue::getValue, KeyValue::getAbsoluteTTL);
    }

    public static KeyDumpItemWriterBuilder client(RedisClient client) {
        return new KeyDumpItemWriterBuilder(client);
    }

    public static KeyDumpItemWriterBuilder client(RedisClusterClient client) {
        return new KeyDumpItemWriterBuilder(client);
    }

    public static class KeyDumpItemWriterBuilder extends CommandBuilder<KeyDumpItemWriterBuilder> {

        public KeyDumpItemWriterBuilder(RedisClusterClient client) {
            super(client);
        }

        public KeyDumpItemWriterBuilder(RedisClient client) {
            super(client);
        }

        public KeyDumpItemWriter build() {
            return new KeyDumpItemWriter(connectionSupplier, poolConfig, async);
        }

    }
}
