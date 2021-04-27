package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class StreamItemReader<K, V> extends ItemStreamSupport implements PollableItemReader<StreamMessage<K, V>> {

    private final Supplier<StatefulConnection<K, V>> connectionSupplier;
    private final Function<StatefulConnection<K, V>, RedisStreamCommands<K, V>> sync;
    private final Long block;
    private final Long count;
    private final boolean noack;
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator;

    public StreamItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier, Function<StatefulConnection<K, V>, RedisStreamCommands<K, V>> sync, StreamOffset<K> offset, Long block, Long count, boolean noack) {
        Assert.notNull(connectionSupplier, "A connection supplier is required");
        Assert.notNull(sync, "A command provider is required");
        Assert.notNull(offset, "Offset is required.");
        this.connectionSupplier = connectionSupplier;
        this.sync = sync;
        this.offset = offset;
        this.block = block;
        this.count = count;
        this.noack = noack;
        this.iterator = Collections.emptyIterator();
    }

    @Override
    public StreamMessage<K, V> read() throws Exception {
        throw new IllegalAccessException("read() method is not supposed to be called");
    }

    @Override
    public StreamMessage<K, V> poll(long timeout, TimeUnit unit) {
        if (!iterator.hasNext()) {
            List<StreamMessage<K, V>> messages = nextMessages(unit.toMillis(timeout));
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
    private List<StreamMessage<K, V>> nextMessages(Long block) {
        XReadArgs args = XReadArgs.Builder.noack(noack);
        if (block != null) {
            args.block(block);
        }
        if (count != null) {
            args.count(count);
        }
        try (StatefulConnection<K, V> connection = connectionSupplier.get()) {
            List<StreamMessage<K, V>> messages = sync.apply(connection).xread(args, offset);
            if (messages != null && !messages.isEmpty()) {
                StreamMessage<K, V> lastMessage = messages.get(messages.size() - 1);
                offset = StreamOffset.from(lastMessage.getStream(), lastMessage.getId());
            }
            return messages;
        }
    }

    public static StreamItemReaderBuilder client(RedisClient client) {
        return new StreamItemReaderBuilder(client);
    }

    public static StreamItemReaderBuilder client(RedisClusterClient client) {
        return new StreamItemReaderBuilder(client);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Setter
    @Accessors(fluent = true)
    public static class StreamItemReaderBuilder extends CommandBuilder<StreamItemReaderBuilder> {

        private XReadArgs.StreamOffset<String> offset;
        private Long block;
        private Long count;
        private boolean noack;

        public StreamItemReaderBuilder(RedisClient client) {
            super(client);
        }

        public StreamItemReaderBuilder(RedisClusterClient client) {
            super(client);
        }

        public StreamItemReader<String, String> build() {
            return new StreamItemReader<String, String>(connectionSupplier, (Function) sync, offset, block, count, noack);
        }
    }

}
