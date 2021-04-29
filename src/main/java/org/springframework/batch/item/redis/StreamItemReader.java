package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class StreamItemReader<K, V> extends ItemStreamSupport implements PollableItemReader<StreamMessage<K, V>> {

    private final Supplier<StatefulConnection<K, V>> connectionSupplier;
    private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
    private final Long count;
    private final Duration block;
    private final StreamOffset<K> initialOffset;
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();

    public StreamItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier, Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, Long count, Duration block, StreamOffset<K> offset) {
        Assert.notNull(connectionSupplier, "A connection supplier is required");
        Assert.notNull(sync, "A command provider is required");
        this.connectionSupplier = connectionSupplier;
        this.sync = sync;
        this.count = count;
        this.block = block;
        this.initialOffset = offset;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        if (offset == null) {
            offset = initialOffset;
        }
        super.open(executionContext);
    }

    @Override
    public StreamMessage<K, V> read() throws Exception {
        throw new IllegalAccessException("read() method is not supposed to be called");
    }

    @Override
    public StreamMessage<K, V> poll(long timeout, TimeUnit unit) {
        if (!iterator.hasNext()) {
            List<StreamMessage<K, V>> messages = nextMessages(Duration.ofMillis(unit.toMillis(timeout)));
            if (messages == null || messages.isEmpty()) {
                return null;
            }
            iterator = messages.iterator();
        }
        return iterator.next();
    }

    @SuppressWarnings("unused")
    public List<StreamMessage<K, V>> readMessages() {
        return nextMessages(block);
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<K, V>> nextMessages(Duration block) {
        XReadArgs args = XReadArgs.Builder.count(count);
        if (block != null) {
            args.block(block);
        }
        try (StatefulConnection<K, V> connection = connectionSupplier.get()) {
            synchronized (connectionSupplier) {
                RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
                List<StreamMessage<K, V>> messages = commands.xread(args, offset);
                if (messages != null && !messages.isEmpty()) {
                    StreamMessage<K, V> lastMessage = messages.get(messages.size() - 1);
                    offset = StreamOffset.from(lastMessage.getStream(), lastMessage.getId());
                }
                return messages;
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

        public StreamItemReaderBuilder offset(StreamOffset<String> offset) {
            return new StreamItemReaderBuilder(client, offset);
        }

    }

    public static class RedisClusterClientStreamItemReaderBuilder {

        private final RedisClusterClient client;

        public RedisClusterClientStreamItemReaderBuilder(RedisClusterClient client) {
            this.client = client;
        }

        public StreamItemReaderBuilder offset(StreamOffset<String> offset) {
            return new StreamItemReaderBuilder(client, offset);
        }

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Setter
    @Accessors(fluent = true)
    public static class StreamItemReaderBuilder extends CommandBuilder<StreamItemReaderBuilder> {

        private static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
        private static final long DEFAULT_COUNT = 50;

        private final StreamOffset<String> offset;
        private Duration block = DEFAULT_BLOCK;
        private Long count = DEFAULT_COUNT;

        public StreamItemReaderBuilder(RedisClient client, StreamOffset<String> offset) {
            super(client);
            this.offset = offset;
        }

        public StreamItemReaderBuilder(RedisClusterClient client, StreamOffset<String> offset) {
            super(client);
            this.offset = offset;
        }

        public StreamItemReader<String, String> build() {
            return new StreamItemReader<>(connectionSupplier, sync, count, block, offset);
        }
    }

}
