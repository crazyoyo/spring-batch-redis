package org.springframework.batch.item.redis;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.ConnectionPoolItemStream;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class StreamItemReader extends ConnectionPoolItemStream<String, String> implements PollableItemReader<StreamMessage<String, String>> {

    private final Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync;
    private final Long count;
    private final Duration block;
    private final List<StreamOffset<String>> initialOffsets;
    private final String consumerGroup;
    private final String consumer;
    private final StreamOffset<String>[] offsets;
    private Iterator<StreamMessage<String, String>> iterator = Collections.emptyIterator();

    @SuppressWarnings("unchecked")
    public StreamItemReader(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync, Long count, Duration block, String consumerGroup, String consumer, List<StreamOffset<String>> offsets) {
        super(connectionSupplier, poolConfig);
        Assert.notNull(sync, "A command provider is required");
        this.sync = sync;
        this.count = count;
        this.block = block;
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.initialOffsets = offsets;
        this.offsets = initialOffsets.stream().map(StreamOffset::getName).map(StreamOffset::lastConsumed).toArray(StreamOffset[]::new);
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            RedisStreamCommands<String, String> commands = (RedisStreamCommands<String, String>) sync.apply(connection);
            XGroupCreateArgs args = XGroupCreateArgs.Builder.mkstream(true);
            for (StreamOffset<String> initialOffset : initialOffsets) {
                try {
                    commands.xgroupCreate(initialOffset, consumerGroup, args);
                } catch (RedisBusyException e) {
                    // Consumer Group name already exists, ignore
                }
            }
        } catch (Exception e) {
            throw new ItemStreamException("Failed to initialize the reader", e);
        }
    }

    @Override
    public StreamMessage<String, String> read() throws Exception {
        throw new IllegalAccessException("read() method is not supposed to be called");
    }

    @Override
    public StreamMessage<String, String> poll(long timeout, TimeUnit unit) throws Exception {
        if (!iterator.hasNext()) {
            List<StreamMessage<String, String>> messages = nextMessages(Duration.ofMillis(unit.toMillis(timeout)));
            if (messages == null || messages.isEmpty()) {
                return null;
            }
            iterator = messages.iterator();
        }
        return iterator.next();
    }

    @SuppressWarnings("unused")
    public List<StreamMessage<String, String>> readMessages() throws Exception {
        return nextMessages(block);
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<String, String>> nextMessages(Duration block) throws Exception {
        XReadArgs args = XReadArgs.Builder.count(count);
        if (block != null) {
            args.block(block);
        }
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            RedisStreamCommands<String, String> commands = (RedisStreamCommands<String, String>) sync.apply(connection);
            Consumer<String> consumer = Consumer.from(consumerGroup, this.consumer);
            return commands.xreadgroup(consumer, args, offsets);
        }
    }

    @SuppressWarnings("unchecked")
    public void ack(List<? extends StreamMessage<String, String>> messages) throws Exception {
        if (messages.isEmpty()) {
            return;
        }
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            RedisStreamCommands<String, String> commands = (RedisStreamCommands<String, String>) sync.apply(connection);
            Map<String, List<StreamMessage<String, String>>> streams = messages.stream().collect(Collectors.groupingBy(StreamMessage::getStream));
            for (String stream : streams.keySet()) {
                String[] messageIds = streams.get(stream).stream().map(StreamMessage::getId).toArray(String[]::new);
                log.info("Ack'ing message ids: {}", Arrays.asList(messageIds));
                commands.xack(stream, consumerGroup, messageIds);
            }
        }
    }

    public static RedisClientStreamItemReaderBuilder client(RedisClient client) {
        return new RedisClientStreamItemReaderBuilder(client);
    }

    public static RedisClusterClientStreamItemReaderBuilder client(RedisClusterClient client) {
        return new RedisClusterClientStreamItemReaderBuilder(client);
    }

    public static class RedisClientStreamItemReaderBuilder {

        private final RedisClient client;

        public RedisClientStreamItemReaderBuilder(RedisClient client) {
            this.client = client;
        }

        public StreamItemReaderBuilder offsets(StreamOffset<String>... offsets) {
            return new StreamItemReaderBuilder(client, Arrays.asList(offsets));
        }

    }

    public static class RedisClusterClientStreamItemReaderBuilder {

        private final RedisClusterClient client;

        public RedisClusterClientStreamItemReaderBuilder(RedisClusterClient client) {
            this.client = client;
        }

        public StreamItemReaderBuilder offsets(StreamOffset<String>... offsets) {
            return new StreamItemReaderBuilder(client, Arrays.asList(offsets));
        }

    }

    @Setter
    @Accessors(fluent = true)
    public static class StreamItemReaderBuilder extends CommandBuilder<String, String, StreamItemReaderBuilder> {

        public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
        public static final long DEFAULT_COUNT = 50;
        public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
        public static final String DEFAULT_CONSUMER = "consumer1";

        private final List<StreamOffset<String>> offsets;
        private Duration block = DEFAULT_BLOCK;
        private Long count = DEFAULT_COUNT;
        private String consumerGroup = DEFAULT_CONSUMER_GROUP;
        private String consumer = DEFAULT_CONSUMER;

        public StreamItemReaderBuilder(RedisClient client, List<StreamOffset<String>> offsets) {
            super(client, StringCodec.UTF8);
            this.offsets = offsets;
        }

        public StreamItemReaderBuilder(RedisClusterClient client, List<StreamOffset<String>> offsets) {
            super(client, StringCodec.UTF8);
            this.offsets = offsets;
        }

        public StreamItemReader build() {
            return new StreamItemReader(connectionSupplier, poolConfig, sync, count, block, consumerGroup, consumer, offsets);
        }
    }

}
