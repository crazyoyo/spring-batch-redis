package org.springframework.batch.item.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.core.convert.converter.Converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class DataStructureItemWriter<K, V, T> extends AbstractPipelineItemWriter<K, V, T> {

    private final Converter<T, K> key;
    private final Converter<T, Object> value;
    private final Converter<T, String> dataType;
    private final Converter<T, Long> absoluteTTL;

    public DataStructureItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, Converter<T, K> key, Converter<T, Object> value, Converter<T, String> dataType, Converter<T, Long> absoluteTTL) {
        super(connectionSupplier, poolConfig, async);
        this.key = key;
        this.value = value;
        this.dataType = dataType;
        this.absoluteTTL = absoluteTTL;
    }

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends T> items) {
        try {
            List<RedisFuture<?>> futures = new ArrayList<>(items.size());
            for (T item : items) {
                K key = this.key.convert(item);
                Object value = this.value.convert(item);
                if (value == null) {
                    futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(key));
                    continue;
                }
                String type = this.dataType.convert(item);
                if (type == null) {
                    continue;
                }
                switch (type.toLowerCase()) {
                    case DataStructure.STRING:
                        futures.add(((RedisStringAsyncCommands<K, V>) commands).set(key, (V) value));
                        break;
                    case DataStructure.LIST:
                        futures.add(((RedisListAsyncCommands<K, V>) commands).rpush(key, (V[]) ((Collection<V>) value).toArray()));
                        break;
                    case DataStructure.SET:
                        futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(key, (V[]) ((Collection<V>) value).toArray()));
                        break;
                    case DataStructure.ZSET:
                        futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, ((Collection<ScoredValue<V>>) value).toArray(new ScoredValue[0])));
                        break;
                    case DataStructure.HASH:
                        futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(key, (Map<K, V>) value));
                        break;
                    case DataStructure.STREAM:
                        Collection<StreamMessage<K, V>> messages = (Collection<StreamMessage<K, V>>) value;
                        for (StreamMessage<K, V> message : messages) {
                            futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(key, new XAddArgs().id(message.getId()), message.getBody()));
                        }
                        break;
                }
                Long ttl = absoluteTTL.convert(item);
                if (ttl == null) {
                    continue;
                }
                if (ttl > 0) {
                    futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key, ttl));
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

        public DataStructureItemWriter<String, String, DataStructure<String>> build() {
            return new DataStructureItemWriter<>(connectionSupplier, poolConfig, async, DataStructure::getKey, DataStructure::getValue, DataStructure::getType, DataStructure::getAbsoluteTTL);
        }

    }

}
