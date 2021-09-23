package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.core.convert.converter.Converter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class DataStructureItemWriter extends AbstractPipelineItemWriter<String, String, DataStructure> {

    private final Converter<StreamMessage<String, String>, XAddArgs> xAddArgs;

    public DataStructureItemWriter(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async, Converter<StreamMessage<String, String>, XAddArgs> xAddArgs) {
        super(connectionSupplier, poolConfig, async);
        this.xAddArgs = xAddArgs;
    }

    @Override
    protected void write(RedisModulesAsyncCommands<String, String> commands, Duration timeout, List<? extends DataStructure> items) {
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (DataStructure ds : items) {
            if (ds == null) {
                continue;
            }
            if (ds.getValue() == null) {
                futures.add(((RedisKeyAsyncCommands<String, String>) commands).del(ds.getKey()));
                continue;
            }
            if (ds.getType() == null) {
                continue;
            }
            switch (ds.getType().toLowerCase()) {
                case DataStructure.HASH:
                    futures.add(commands.del(ds.getKey()));
                    futures.add(commands.hset(ds.getKey(), (Map<String, String>) ds.getValue()));
                    break;
                case DataStructure.STRING:
                    futures.add(commands.set(ds.getKey(), (String) ds.getValue()));
                    break;
                case DataStructure.LIST:
                    flush(commands, timeout, commands.del(ds.getKey()), commands.rpush(ds.getKey(), ((Collection<String>) ds.getValue()).toArray(new String[0])));
                    break;
                case DataStructure.SET:
                    flush(commands, timeout, commands.del(ds.getKey()), commands.sadd(ds.getKey(), ((Collection<String>) ds.getValue()).toArray(new String[0])));
                    break;
                case DataStructure.ZSET:
                    flush(commands, timeout, commands.del(ds.getKey()), commands.zadd(ds.getKey(), ((Collection<ScoredValue<String>>) ds.getValue()).toArray(new ScoredValue[0])));
                    break;
                case DataStructure.STREAM:
                    List<RedisFuture<?>> streamFutures = new ArrayList<>();
                    streamFutures.add(commands.del(ds.getKey()));
                    Collection<StreamMessage<String, String>> messages = (Collection<StreamMessage<String, String>>) ds.getValue();
                    for (StreamMessage<String, String> message : messages) {
                        streamFutures.add(commands.xadd(ds.getKey(), xAddArgs.convert(message), message.getBody()));
                    }
                    flush(commands, timeout, streamFutures);
                    break;
            }
            if (ds.getAbsoluteTTL() > 0) {
                futures.add(commands.pexpireat(ds.getKey(), ds.getAbsoluteTTL()));
            }
        }
        flush(commands, timeout, futures);
    }

    public static DataStructureItemWriterBuilder client(RedisModulesClient client) {
        return new DataStructureItemWriterBuilder(client);
    }

    public static DataStructureItemWriterBuilder client(RedisModulesClusterClient client) {
        return new DataStructureItemWriterBuilder(client);
    }

    @Setter
    @Accessors(fluent = true)
    public static class DataStructureItemWriterBuilder extends CommandBuilder<String, String, DataStructureItemWriterBuilder> {

        private Converter<StreamMessage<String, String>, XAddArgs> xAddArgs = m -> new XAddArgs().id(m.getId());

        public DataStructureItemWriterBuilder(AbstractRedisClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataStructureItemWriter build() {
            return new DataStructureItemWriter(connectionSupplier(), poolConfig, async(), xAddArgs);
        }

    }

}
