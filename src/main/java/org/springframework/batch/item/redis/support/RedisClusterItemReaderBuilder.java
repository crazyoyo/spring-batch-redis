package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisItemReader;

@Setter
@Accessors(fluent = true)
public class RedisClusterItemReaderBuilder extends AbstractRedisItemReaderBuilder {

    private RedisOptions redisOptions = RedisOptions.builder().build();
    private ReaderOptions readerOptions = ReaderOptions.builder().build();
    private ClusterClientOptions clientOptions;

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
        RedisClusterClient client = redisOptions.client(clientOptions);
        return reader(scanReader(client.connect(), StatefulRedisClusterConnection::sync), keyValueProcessor(redisOptions.connectionPool(client), StatefulRedisClusterConnection::async));
    }

    public RedisItemReader<String, KeyValue<String>> liveValueReader() {
        validate();
        RedisClusterClient client = redisOptions.client(clientOptions);
        return reader(continuousKeyReader(client), keyValueProcessor(redisOptions.connectionPool(client), StatefulRedisClusterConnection::async));
    }

    public RedisItemReader<String, KeyDump<String>> dumpReader() {
        validate();
        RedisClusterClient client = redisOptions.client(clientOptions);
        return reader(scanReader(client.connect(), StatefulRedisClusterConnection::sync), keyDumpProcessor(redisOptions.connectionPool(client), StatefulRedisClusterConnection::async));
    }

    public RedisItemReader<String, KeyDump<String>> liveDumpReader() {
        validate();
        RedisClusterClient client = redisOptions.client(clientOptions);
        return reader(continuousKeyReader(client), keyDumpProcessor(redisOptions.connectionPool(client), StatefulRedisClusterConnection::async));
    }


    private ItemReader<String> continuousKeyReader(RedisClusterClient client) {
        return multiplexingReader(keyspaceNotificationReader(client), scanReader(client.connect(), StatefulRedisClusterConnection::sync));
    }


    private ItemReader<String> keyspaceNotificationReader(RedisClusterClient client) {
        return RedisClusterKeyspaceNotificationItemReader.<String, String>builder().connection(client.connectPubSub()).queue(queue()).pollingTimeout(getQueuePollingTimeout()).patterns(getKeyspacePatterns()).keyExtractor(KEY_EXTRACTOR).build();
    }
}
