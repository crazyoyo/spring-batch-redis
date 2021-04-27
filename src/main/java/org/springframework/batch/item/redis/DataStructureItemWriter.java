package org.springframework.batch.item.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.DataStructure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class DataStructureItemWriter<K, V> extends AbstractPipelineItemWriter<K, V, DataStructure<K>> {

    public DataStructureItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
        super(connectionSupplier, poolConfig, async);
    }

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends DataStructure<K>> items) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            List<RedisFuture<?>> futures = new ArrayList<>(items.size());
            for (DataStructure<K> item : items) {
                if (item.getValue() == null) {
                    futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey()));
                    continue;
                }
                switch (item.getType().toLowerCase()) {
                    case DataStructure.STRING:
                        futures.add(((RedisStringAsyncCommands<K, V>) commands).set(item.getKey(), (V) item.getValue()));
                        break;
                    case DataStructure.LIST:
                        futures.add(((RedisListAsyncCommands<K, V>) commands).rpush(item.getKey(), (V[]) ((Collection<V>) item.getValue()).toArray()));
                        break;
                    case DataStructure.SET:
                        futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(item.getKey(), (V[]) ((Collection<V>) item.getValue()).toArray()));
                        break;
                    case DataStructure.ZSET:
                        futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(item.getKey(), ((Collection<ScoredValue<V>>) item.getValue()).toArray(new ScoredValue[0])));
                        break;
                    case DataStructure.HASH:
                        futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(item.getKey(), (Map<K, V>) item.getValue()));
                        break;
                    case DataStructure.STREAM:
                        Collection<StreamMessage<K, V>> messages = (Collection<StreamMessage<K, V>>) item.getValue();
                        for (StreamMessage<K, V> message : messages) {
                            futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(item.getKey(), new XAddArgs().id(message.getId()), message.getBody()));
                        }
                        break;
                }
                if (item.getAbsoluteTTL() > 0) {
                    futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(item.getKey(), item.getAbsoluteTTL()));
                }
            }
            commands.flushCommands();
            LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
        } finally {
            commands.setAutoFlushCommands(true);
        }
    }

    public static DataStructureItemWriterBuilder client(RedisClient client) {
        return new DataStructureItemWriterBuilder(client);
    }

    public static DataStructureItemWriterBuilder client(RedisClusterClient client) {
        return new DataStructureItemWriterBuilder(client);
    }

    public static class DataStructureItemWriterBuilder extends CommandBuilder<DataStructureItemWriterBuilder> {

        public DataStructureItemWriterBuilder(RedisClusterClient client) {
            super(client);
        }

        public DataStructureItemWriterBuilder(RedisClient client) {
            super(client);
        }

        public DataStructureItemWriter<String, String> build() {
            return new DataStructureItemWriter<>(connectionSupplier, poolConfig, async);
        }

    }

}
