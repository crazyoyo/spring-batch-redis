package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.core.convert.converter.Converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class DataStructureItemWriter<T> extends AbstractPipelineItemWriter<String, String, T> {

    private final Converter<T, String> key;
    private final Converter<T, Object> value;
    private final Converter<T, String> dataType;
    private final Converter<T, Long> absoluteTTL;

    public DataStructureItemWriter(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async, Converter<T, String> key, Converter<T, Object> value, Converter<T, String> dataType, Converter<T, Long> absoluteTTL) {
        super(connectionSupplier, poolConfig, async);
        this.key = key;
        this.value = value;
        this.dataType = dataType;
        this.absoluteTTL = absoluteTTL;
    }

    @Override
    protected void write(RedisModulesAsyncCommands<String, String> commands, long timeout, List<? extends T> items) {
        try {
            List<RedisFuture<?>> futures = new ArrayList<>(items.size());
            for (T item : items) {
                String key = this.key.convert(item);
                Object value = this.value.convert(item);
                if (value == null) {
                    futures.add(((RedisKeyAsyncCommands<String, String>) commands).del(key));
                    continue;
                }
                String type = this.dataType.convert(item);
                if (type == null) {
                    continue;
                }
                switch (type.toLowerCase()) {
                    case DataStructure.STRING:
                        futures.add(commands.set(key, (String) value));
                        break;
                    case DataStructure.LIST:
                        futures.add(commands.rpush(key, ((Collection<String>) value).toArray(new String[0])));
                        break;
                    case DataStructure.SET:
                        futures.add(commands.sadd(key, ((Collection<String>) value).toArray(new String[0])));
                        break;
                    case DataStructure.ZSET:
                        futures.add(commands.zadd(key, ((Collection<ScoredValue<String>>) value).toArray(new ScoredValue[0])));
                        break;
                    case DataStructure.HASH:
                        futures.add(commands.hset(key, (Map<String, String>) value));
                        break;
                    case DataStructure.STREAM:
                        Collection<StreamMessage<String, String>> messages = (Collection<StreamMessage<String, String>>) value;
                        for (StreamMessage<String, String> message : messages) {
                            futures.add(commands.xadd(key, new XAddArgs().id(message.getId()), message.getBody()));
                        }
                        break;
                }
                Long ttl = absoluteTTL.convert(item);
                if (ttl == null) {
                    continue;
                }
                if (ttl > 0) {
                    futures.add(commands.pexpireat(key, ttl));
                }
            }
            commands.flushCommands();
            LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
        } finally {
            commands.setAutoFlushCommands(true);
        }
    }

    public static DataStructureItemWriterBuilder client(RedisModulesClient client) {
        return new DataStructureItemWriterBuilder(client);
    }

    public static DataStructureItemWriterBuilder client(RedisModulesClusterClient client) {
        return new DataStructureItemWriterBuilder(client);
    }

    public static class DataStructureItemWriterBuilder extends CommandBuilder<String,String,DataStructureItemWriterBuilder> {

        public DataStructureItemWriterBuilder(RedisModulesClusterClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataStructureItemWriterBuilder(RedisModulesClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataStructureItemWriter<DataStructure> build() {
            return new DataStructureItemWriter<>(connectionSupplier(), poolConfig, async(), DataStructure::getKey, DataStructure::getValue, DataStructure::getType, DataStructure::getAbsoluteTTL);
        }

    }

}
