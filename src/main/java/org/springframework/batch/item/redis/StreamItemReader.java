package org.springframework.batch.item.redis;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.redis.support.AbstractPollableItemReader;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamItemReader<K, V> extends AbstractPollableItemReader<StreamMessage<K, V>> {

    private final RedisStreamCommands<K, V> commands;
    private final Long count;
    private final boolean noack;
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator;

    public StreamItemReader(RedisStreamCommands<K, V> commands, StreamOffset<K> offset, Long count, boolean noack) {
        Assert.notNull(commands, "A RedisStreamCommands instance is required.");
        Assert.notNull(offset, "Offset is required.");
        this.commands = commands;
        this.offset = offset;
        this.count = count;
        this.noack = noack;
    }

    @Override
    protected void doOpen() {
        iterator = Collections.emptyIterator();
    }

    @Override
    protected void doClose() {
        iterator = null;
    }

    @Override
    public StreamMessage<K, V> poll(long timeout, TimeUnit unit) {
        if (!iterator.hasNext()) {
            XReadArgs args = XReadArgs.Builder.block(unit.toMillis(timeout)).noack(noack);
            if (count != null) {
                args.count(count);
            }
            List<StreamMessage<K, V>> messages = commands.xread(args, offset);
            if (messages == null || messages.isEmpty()) {
                return null;
            }
            iterator = messages.iterator();
        }
        StreamMessage<K, V> message = iterator.next();
        offset = StreamOffset.from(message.getStream(), message.getId());
        log.debug("Message: {}", message);
        return message;
    }

    @Setter
    @Accessors(fluent = true)
    public static abstract class StreamItemReaderBuilder<K, V> {

        protected XReadArgs.StreamOffset<K> offset;
        protected Long count;
        protected boolean noack;

        public abstract StreamItemReader<K, V> build();

    }

    public static <K, V> StreamItemReaderBuilder<K, V> builder(StatefulRedisConnection<K, V> connection) {
        return new StreamItemReaderBuilder<K, V>() {
            @Override
            public StreamItemReader<K, V> build() {
                return new StreamItemReader<>(connection.sync(), offset, count, noack);
            }
        };
    }

    public static <K, V> StreamItemReaderBuilder<K, V> builder(StatefulRedisClusterConnection<K, V> connection) {
        return new StreamItemReaderBuilder<K, V>() {
            @Override
            public StreamItemReader<K, V> build() {
                return new StreamItemReader<>(connection.sync(), offset, count, noack);
            }
        };
    }


}
