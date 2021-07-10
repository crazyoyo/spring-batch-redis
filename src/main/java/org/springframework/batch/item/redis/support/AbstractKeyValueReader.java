package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.FileCopyUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractKeyValueReader<T extends KeyValue<?>> extends ConnectionPoolItemStream<String, String> implements ItemProcessor<List<? extends String>, List<T>> {

    private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;
    private String digest;

    protected AbstractKeyValueReader(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
        super(connectionSupplier, poolConfig);
        this.async = async;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (digest == null) {
            try (StatefulConnection<String, String> connection = pool.borrowObject()) {
                long timeout = connection.getTimeout().toMillis();
                RedisScriptingAsyncCommands<String, String> commands = (RedisScriptingAsyncCommands<String, String>) async.apply(connection);
                byte[] bytes = FileCopyUtils.copyToByteArray(getClass().getClassLoader().getResourceAsStream("absttl.lua"));
                RedisFuture<String> load = commands.scriptLoad(bytes);
                this.digest = load.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new ItemStreamException("Could not open reader", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected RedisFuture<Long> absoluteTTL(BaseRedisAsyncCommands<String, String> commands, String key) {
        return ((RedisScriptingAsyncCommands<String, String>) commands).evalsha(digest, ScriptOutputType.INTEGER, key);
    }

    @Override
    public List<T> process(List<? extends String> keys) throws Exception {
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                return read(commands, connection.getTimeout().toMillis(), keys);
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract List<T> read(BaseRedisAsyncCommands<String, String> commands, long timeout, List<? extends String> keys) throws InterruptedException, ExecutionException, TimeoutException;

}
