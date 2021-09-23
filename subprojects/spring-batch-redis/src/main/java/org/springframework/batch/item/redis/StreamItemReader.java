package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
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

    private final Function<StatefulConnection<String, String>, RedisModulesCommands<String, String>> sync;
    private final Long count;
    private final Duration block;
    private final StreamOffset<String> offset;
    private final String consumerGroup;
    private final String consumer;
    private final AckPolicy ackPolicy;
    private Iterator<StreamMessage<String, String>> iterator = Collections.emptyIterator();

    public StreamItemReader(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, RedisModulesCommands<String, String>> sync, Long count, Duration block, String consumerGroup, String consumer, StreamOffset<String> offset, AckPolicy ackPolicy) {
        super(connectionSupplier, poolConfig);
        Assert.notNull(sync, "A command provider is required");
        this.sync = sync;
        this.count = count;
        this.block = block;
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.offset = offset;
        this.ackPolicy = ackPolicy;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            RedisStreamCommands<String, String> commands = sync.apply(connection);
            XGroupCreateArgs args = XGroupCreateArgs.Builder.mkstream(true);
            try {
                commands.xgroupCreate(offset, consumerGroup, args);
            } catch (RedisBusyException e) {
                // Consumer Group name already exists, ignore
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
            List<StreamMessage<String, String>> messages = readMessages(Duration.ofMillis(unit.toMillis(timeout)));
            if (messages == null || messages.isEmpty()) {
                return null;
            }
            iterator = messages.iterator();
        }
        return iterator.next();
    }

    @SuppressWarnings("unused")
    public List<StreamMessage<String, String>> readMessages() throws Exception {
        return readMessages(block);
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<String, String>> readMessages(Duration block) throws Exception {
        XReadArgs args = XReadArgs.Builder.count(count);
        if (block != null) {
            args.block(block);
        }
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            RedisStreamCommands<String, String> commands = sync.apply(connection);
            List<StreamMessage<String, String>> messages = commands.xreadgroup(Consumer.from(consumerGroup, consumer), args, StreamOffset.lastConsumed(offset.getName()));
            if (ackPolicy == AckPolicy.AUTO) {
                ack(messages);
            }
            return messages;
        }
    }

    public void ack(List<? extends StreamMessage<String, String>> messages) throws Exception {
        if (messages.isEmpty()) {
            return;
        }
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            RedisStreamCommands<String, String> commands = sync.apply(connection);
            Map<String, List<StreamMessage<String, String>>> streams = messages.stream().collect(Collectors.groupingBy(StreamMessage::getStream));
            for (Map.Entry<String, List<StreamMessage<String, String>>> entry : streams.entrySet()) {
                String[] messageIds = entry.getValue().stream().map(StreamMessage::getId).toArray(String[]::new);
                log.info("Ack'ing message ids: {}", Arrays.asList(messageIds));
                commands.xack(entry.getKey(), consumerGroup, messageIds);
            }
        }
    }

    public enum AckPolicy {
        AUTO, MANUAL
    }

    public static OffsetStreamItemReaderBuilder client(RedisModulesClient client) {
        return new OffsetStreamItemReaderBuilder(client);
    }

    public static OffsetStreamItemReaderBuilder client(RedisModulesClusterClient client) {
        return new OffsetStreamItemReaderBuilder(client);
    }

    public static class OffsetStreamItemReaderBuilder {

        private final AbstractRedisClient client;

        public OffsetStreamItemReaderBuilder(AbstractRedisClient client) {
            this.client = client;
        }

        public StreamItemReaderBuilder offset(StreamOffset<String> offset) {
            return new StreamItemReaderBuilder(client, offset);
        }

    }

    @Setter
    @Accessors(fluent = true)
    public static class StreamItemReaderBuilder extends CommandBuilder<String, String, StreamItemReaderBuilder> {

        public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
        public static final long DEFAULT_COUNT = 50;
        public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
        public static final String DEFAULT_CONSUMER = "consumer1";
        public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.AUTO;

        private final StreamOffset<String> offset;
        private Duration block = DEFAULT_BLOCK;
        private Long count = DEFAULT_COUNT;
        private String consumerGroup = DEFAULT_CONSUMER_GROUP;
        private String consumer = DEFAULT_CONSUMER;
        private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;

        public StreamItemReaderBuilder(AbstractRedisClient client, StreamOffset<String> offset) {
            super(client, StringCodec.UTF8);
            this.offset = offset;
        }

        public StreamItemReader build() {
            return new StreamItemReader(connectionSupplier(), poolConfig, sync(), count, block, consumerGroup, consumer, offset, ackPolicy);
        }
    }

}
