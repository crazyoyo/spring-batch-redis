package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisItemReader;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractRedisItemReaderBuilder {

    public static final String DATABASE_TOKEN = "database";
    public static final String KEYSPACE_CHANNEL_TEMPLATE = "__keyspace@${" + DATABASE_TOKEN + "}__:*";
    protected static final BiFunction<String, String, String> KEY_EXTRACTOR = (c, m) -> c.substring(c.indexOf(":") + 1);

    protected long getQueuePollingTimeout() {
        return getReaderOptions().getQueuePollingTimeout();
    }

    protected abstract RedisOptions getRedisOptions();

    protected abstract ReaderOptions getReaderOptions();

    protected void validate() {
        Assert.notNull(getRedisOptions(), "Redis options are required.");
        Assert.notNull(getReaderOptions(), "Reader options are required.");
    }

    protected ItemReader<String> multiplexingReader(ItemReader<String>... readers) {
        return MultiplexingItemReader.<String>builder().readers(Arrays.asList(readers)).queue(queue()).pollingTimeout(getQueuePollingTimeout()).build();
    }

    protected <C extends StatefulConnection<String, String>> ItemProcessor<List<? extends String>, List<? extends KeyValue<String>>> keyValueProcessor(GenericObjectPool<C> connectionPool, Function<C, BaseRedisAsyncCommands<String, String>> commands) {
        return KeyValueItemProcessor.<String, String, C>builder().pool(connectionPool).commands(commands).commandTimeout(getCommandTimeout()).build();
    }

    protected <C extends StatefulConnection<String, String>> ItemProcessor<List<? extends String>, List<? extends KeyDump<String>>> keyDumpProcessor(GenericObjectPool<C> connectionPool, Function<C, BaseRedisAsyncCommands<String, String>> commands) {
        return KeyDumpItemProcessor.<String, String, C>builder().pool(connectionPool).commands(commands).commandTimeout(getCommandTimeout()).build();
    }

    protected <C extends StatefulConnection<String, String>> ItemReader<String> scanReader(C connection, Function<C, RedisKeyCommands<String, String>> commands) {
        return ScanItemReader.<String, String, C>builder().connection(connection).commands(commands).scanArgs(getReaderOptions().getScanArgs()).build();
    }

    private long getCommandTimeout() {
        return getRedisOptions().getRedisURI().getTimeout().getSeconds();
    }

    protected String[] getKeyspacePatterns() {
        if (getReaderOptions().getKeyspacePatterns() == null || getReaderOptions().getKeyspacePatterns().isEmpty()) {
            Map<String, String> variables = new HashMap<>();
            variables.put(DATABASE_TOKEN, String.valueOf(getRedisOptions().getRedisURI().getDatabase()));
            StringSubstitutor substitutor = new StringSubstitutor(variables);
            return new String[]{substitutor.replace(KEYSPACE_CHANNEL_TEMPLATE)};
        }
        return getReaderOptions().getKeyspacePatterns().toArray(new String[0]);
    }

    protected <T> RedisItemReader<String, T> reader(ItemReader<String> keyReader, ItemProcessor<List<? extends String>, List<? extends T>> valueReader) {
        return new RedisItemReader<>(keyReader, valueReader, getReaderOptions());
    }

    protected <T> BlockingQueue<T> queue() {
        return new LinkedBlockingDeque<>(getReaderOptions().getQueueCapacity());
    }


}
