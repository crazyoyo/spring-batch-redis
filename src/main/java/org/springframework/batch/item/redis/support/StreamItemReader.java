package org.springframework.batch.item.redis.support;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class StreamItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractPollableItemReader<StreamMessage<K, V>> {

    private final C connection;
    private final Function<C, RedisStreamCommands<K, V>> commands;
    private final Long count;
    private final boolean noack;
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator;

    public StreamItemReader(C connection, Function<C, RedisStreamCommands<K, V>> commands, StreamOffset<K> offset, Long count, boolean noack) {
        Assert.notNull(connection, "A Redis connection is required.");
        Assert.notNull(commands, "A command provider is required");
        Assert.notNull(offset, "Offset is required.");
        this.connection = connection;
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

    @SuppressWarnings("unchecked")
    @Override
    public StreamMessage<K, V> poll(long timeout, TimeUnit unit) {
        if (!iterator.hasNext()) {
            XReadArgs args = XReadArgs.Builder.block(unit.toMillis(timeout)).noack(noack);
            if (count != null) {
                args.count(count);
            }
            List<StreamMessage<K, V>> messages = commands.apply(connection).xread(args, offset);
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

    @SuppressWarnings("rawtypes")
    @Setter
    @Accessors(fluent = true)
    public static abstract class StreamItemReaderBuilder<K, R extends StreamItemReader> {

        protected XReadArgs.StreamOffset<K> offset;
        protected Long count;
        protected boolean noack;

        public abstract R build();

    }

}
