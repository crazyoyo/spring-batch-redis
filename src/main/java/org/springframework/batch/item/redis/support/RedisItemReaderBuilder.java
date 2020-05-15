package org.springframework.batch.item.redis.support;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisItemReader;

@Setter
@Accessors(fluent = true)
public class RedisItemReaderBuilder extends AbstractRedisItemReaderBuilder {

    private RedisOptions redisOptions = RedisOptions.builder().build();
    private ReaderOptions readerOptions = ReaderOptions.builder().build();
    private ClientOptions clientOptions;

    @Override
    public RedisOptions getRedisOptions() {
        return redisOptions;
    }

    @Override
    public ReaderOptions getReaderOptions() {
        return readerOptions;
    }

    public RedisItemReader<String, KeyValue<String>> valueReader() {
        validate();
        RedisClient client = redisOptions.client(clientOptions);
        return reader(scanReader(client.connect(), StatefulRedisConnection::sync), keyValueProcessor(redisOptions.connectionPool(client), StatefulRedisConnection::async));
    }

    public RedisItemReader<String, KeyValue<String>> liveValueReader() {
        validate();
        RedisClient client = redisOptions.client(clientOptions);
        return reader(continuousKeyReader(client), keyValueProcessor(redisOptions.connectionPool(client), StatefulRedisConnection::async));
    }

    public RedisItemReader<String, KeyDump<String>> dumpReader() {
        validate();
        RedisClient client = redisOptions.client(clientOptions);
        return reader(scanReader(client.connect(), StatefulRedisConnection::sync), keyDumpProcessor(redisOptions.connectionPool(client), StatefulRedisConnection::async));
    }

    public RedisItemReader<String, KeyDump<String>> liveDumpReader() {
        validate();
        RedisClient client = redisOptions.client(clientOptions);
        return reader(continuousKeyReader(client), keyDumpProcessor(redisOptions.connectionPool(client), StatefulRedisConnection::async));
    }

    private ItemReader<String> continuousKeyReader(RedisClient client) {
        return multiplexingReader(keyspaceNotificationReader(client), scanReader(client.connect(), StatefulRedisConnection::sync));
    }

    private ItemReader<String> keyspaceNotificationReader(RedisClient client) {
        return RedisKeyspaceNotificationItemReader.<String, String>builder().connection(client.connectPubSub()).queue(queue()).pollingTimeout(getQueuePollingTimeout()).patterns(getKeyspacePatterns()).keyExtractor(KEY_EXTRACTOR).build();
    }

}
