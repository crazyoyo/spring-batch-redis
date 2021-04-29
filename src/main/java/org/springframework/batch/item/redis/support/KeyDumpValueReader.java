package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

public class KeyDumpValueReader<K, V> extends AbstractKeyValueReader<K, V, KeyValue<K, byte[]>> {

    public KeyDumpValueReader(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
        super(connectionSupplier, poolConfig, async);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<KeyValue<K, byte[]>> read(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends K> keys) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            ttlFutures.add(absoluteTTL(commands, key));
            dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyValue<K, byte[]>> dumps = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            K key = keys.get(index);
            long absoluteTTL = ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
            byte[] bytes = dumpFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
            dumps.add(new KeyValue<>(key, absoluteTTL, bytes));
        }
        return dumps;
    }

    public static KeyDumpValueReaderBuilder client(RedisClient client) {
        return new KeyDumpValueReaderBuilder(client);
    }

    public static KeyDumpValueReaderBuilder client(RedisClusterClient client) {
        return new KeyDumpValueReaderBuilder(client);
    }

    public static class KeyDumpValueReaderBuilder extends CommandBuilder<KeyDumpValueReaderBuilder> {

        public KeyDumpValueReaderBuilder(RedisClusterClient client) {
            super(client);
        }

        public KeyDumpValueReaderBuilder(RedisClient client) {
            super(client);
        }

        public KeyDumpValueReader<String, String> build() {
            return new KeyDumpValueReader<>(connectionSupplier, poolConfig, async);
        }
    }


}
