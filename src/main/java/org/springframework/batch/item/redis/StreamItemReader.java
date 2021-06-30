package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import lombok.Getter;
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
public class StreamItemReader extends ItemStreamSupport implements PollableItemReader<StreamMessage<String, String>> {

    private final Supplier<StatefulConnection<String, String>> connectionSupplier;
    private final Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync;
    private final Long count;
    private final Duration block;
    private final StreamOffset<String> initialOffset;
    @Getter
    private StreamOffset<String> offset;
    private Iterator<StreamMessage<String, String>> iterator = Collections.emptyIterator();

    public StreamItemReader(Supplier<StatefulConnection<String, String>> connectionSupplier, Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync, Long count, Duration block, StreamOffset<String> offset) {
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
    public StreamMessage<String, String> read() throws Exception {
        throw new IllegalAccessException("read() method is not supposed to be called");
    }

    @Override
    public StreamMessage<String, String> poll(long timeout, TimeUnit unit) {
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
    public List<StreamMessage<String, String>> readMessages() {
        return nextMessages(block);
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<String, String>> nextMessages(Duration block) {
        XReadArgs args = XReadArgs.Builder.count(count);
        if (block != null) {
            args.block(block);
        }
        try (StatefulConnection<String, String> connection = connectionSupplier.get()) {
            synchronized (connectionSupplier) {
                RedisStreamCommands<String, String> commands = (RedisStreamCommands<String, String>) sync.apply(connection);
                List<StreamMessage<String, String>> messages = commands.xread(args, offset);
                if (messages != null && !messages.isEmpty()) {
                    StreamMessage<String, String> lastMessage = messages.get(messages.size() - 1);
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

    @Setter
    @Accessors(fluent = true)
    public static class StreamItemReaderBuilder extends CommandBuilder<String, String, StreamItemReaderBuilder> {

        private static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
        private static final long DEFAULT_COUNT = 50;

        private final StreamOffset<String> offset;
        private Duration block = DEFAULT_BLOCK;
        private Long count = DEFAULT_COUNT;

        public StreamItemReaderBuilder(RedisClient client, StreamOffset<String> offset) {
            super(client, StringCodec.UTF8);
            this.offset = offset;
        }

        public StreamItemReaderBuilder(RedisClusterClient client, StreamOffset<String> offset) {
            super(client, StringCodec.UTF8);
            this.offset = offset;
        }

        public StreamItemReader build() {
            return new StreamItemReader(connectionSupplier, sync, count, block, offset);
        }
    }

}
