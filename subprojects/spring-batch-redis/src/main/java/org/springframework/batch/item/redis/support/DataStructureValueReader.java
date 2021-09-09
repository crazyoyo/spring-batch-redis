package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

public class DataStructureValueReader extends AbstractValueReader<DataStructure> {

    public DataStructureValueReader(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
        super(connectionSupplier, poolConfig, async);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<DataStructure> read(BaseRedisAsyncCommands<String, String> commands, long timeout, List<? extends String> keys) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
        for (String key : keys) {
            typeFutures.add(((RedisKeyAsyncCommands<String, String>) commands).type(key));
        }
        commands.flushCommands();
        List<DataStructure> dataStructures = new ArrayList<>(keys.size());
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            String key = keys.get(index);
            String type = typeFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
            valueFutures.add(value(commands, key, type));
            ttlFutures.add(absoluteTTL(commands, key));
            dataStructures.add(new DataStructure(key, type));
        }
        commands.flushCommands();
        for (int index = 0; index < dataStructures.size(); index++) {
            DataStructure dataStructure = dataStructures.get(index);
            RedisFuture<?> valueFuture = valueFutures.get(index);
            if (valueFuture != null) {
                dataStructure.setValue(valueFuture.get(timeout, TimeUnit.MILLISECONDS));
            }
            long absoluteTTL = ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS);
            dataStructure.setAbsoluteTTL(absoluteTTL);
        }
        return dataStructures;
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<?> value(BaseRedisAsyncCommands<String, String> commands, String key, String type) {
        switch (type.toLowerCase()) {
            case DataStructure.HASH:
                return ((RedisHashAsyncCommands<String, String>) commands).hgetall(key);
            case DataStructure.LIST:
                return ((RedisListAsyncCommands<String, String>) commands).lrange(key, 0, -1);
            case DataStructure.SET:
                return ((RedisSetAsyncCommands<String, String>) commands).smembers(key);
            case DataStructure.STREAM:
                return ((RedisStreamAsyncCommands<String, String>) commands).xrange(key, Range.create("-", "+"));
            case DataStructure.STRING:
                return ((RedisStringAsyncCommands<String, String>) commands).get(key);
            case DataStructure.ZSET:
                return ((RedisSortedSetAsyncCommands<String, String>) commands).zrangeWithScores(key, 0, -1);
            default:
                return null;
        }
    }

    public static DataStructureValueReaderBuilder client(RedisClient client) {
        return new DataStructureValueReaderBuilder(client);
    }

    public static DataStructureValueReaderBuilder client(RedisClusterClient client) {
        return new DataStructureValueReaderBuilder(client);
    }

    public static class DataStructureValueReaderBuilder extends CommandBuilder<String, String, DataStructureValueReaderBuilder> {

        public DataStructureValueReaderBuilder(RedisClusterClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataStructureValueReaderBuilder(RedisClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataStructureValueReader build() {
            return new DataStructureValueReader(connectionSupplier(), poolConfig, async());
        }
    }


}
