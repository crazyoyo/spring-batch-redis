package org.springframework.batch.item.redis.support;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

public class KeyDumpValueReader extends AbstractValueReader<KeyValue<byte[]>> {

    public KeyDumpValueReader(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async) {
        super(connectionSupplier, poolConfig, async);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<KeyValue<byte[]>> read(BaseRedisAsyncCommands<String, String> commands, long timeout, List<? extends String> keys) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
        for (String key : keys) {
            ttlFutures.add(absoluteTTL(commands, key));
            dumpFutures.add(((RedisKeyAsyncCommands<String, String>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyValue<byte[]>> dumps = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            String key = keys.get(index);
            long absoluteTTL = ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
            byte[] bytes = dumpFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
            dumps.add(new KeyValue<>(key, absoluteTTL, bytes));
        }
        return dumps;
    }

    public static KeyDumpValueReaderBuilder client(RedisModulesClient client) {
        return new KeyDumpValueReaderBuilder(client);
    }

    public static KeyDumpValueReaderBuilder client(RedisModulesClusterClient client) {
        return new KeyDumpValueReaderBuilder(client);
    }

    public static class KeyDumpValueReaderBuilder extends CommandBuilder<String, String, KeyDumpValueReaderBuilder> {

        public KeyDumpValueReaderBuilder(AbstractRedisClient client) {
            super(client, StringCodec.UTF8);
        }

        public KeyDumpValueReader build() {
            return new KeyDumpValueReader(connectionSupplier(), poolConfig, async());
        }
    }


}
