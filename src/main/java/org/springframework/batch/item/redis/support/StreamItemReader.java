package org.springframework.batch.item.redis.support;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class StreamItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractPollableItemReader<StreamMessage<K, V>> {

    private final C connection;
    private final Function<C, RedisStreamCommands<K, V>> commands;
    private final Long block;
    private final Long count;
    private final boolean noack;
    @Getter
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator;

    public StreamItemReader(Duration readTimeout, C connection, Function<C, RedisStreamCommands<K, V>> commands, StreamOffset<K> offset, Long block, Long count, boolean noack) {
        super(readTimeout);
        Assert.notNull(connection, "A Redis connection is required.");
        Assert.notNull(commands, "A command provider is required");
        Assert.notNull(offset, "Offset is required.");
        this.connection = connection;
        this.commands = commands;
        this.offset = offset;
        this.block = block;
        this.count = count;
        this.noack = noack;
        this.iterator = Collections.emptyIterator();
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
        List<StreamMessage<K, V>> messages = commands.apply(connection).xread(args, offset);
        if (messages != null && !messages.isEmpty()) {
            StreamMessage<K, V> lastMessage = messages.get(messages.size() - 1);
            offset = StreamOffset.from(lastMessage.getStream(), lastMessage.getId());
        }
        return messages;
    }

    @SuppressWarnings({"unchecked", "unused"})
    public static abstract class StreamItemReaderBuilder<K, V, R extends StreamItemReader<K, V, ?>, B extends StreamItemReaderBuilder<K, V, R, B>> extends PollableItemReaderBuilder<B> {

        protected XReadArgs.StreamOffset<K> offset;
        protected Long block;
        protected Long count;
        protected boolean noack;

        public B offset(XReadArgs.StreamOffset<K> offset) {
            this.offset = offset;
            return (B) this;
        }

        public B block(Long block) {
            this.block = block;
            return (B) this;
        }

        public B count(Long count) {
            this.count = count;
            return (B) this;
        }

        public B noack(boolean noack) {
            this.noack = noack;
            return (B) this;
        }

        public abstract R build();

    }

}
