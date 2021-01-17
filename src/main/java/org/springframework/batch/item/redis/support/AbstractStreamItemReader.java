package org.springframework.batch.item.redis.support;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractStreamItemReader<K, V> extends AbstractPollableItemReader<StreamMessage<K, V>> {

    private final Long count;
    private final boolean noack;
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator;

    public AbstractStreamItemReader(StreamOffset<K> offset, Long count, boolean noack) {
        Assert.notNull(offset, "Offset is required.");
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
            List<StreamMessage<K, V>> messages = commands().xread(args, offset);
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

    protected abstract RedisStreamCommands<K, V> commands();

    public static class StreamItemReaderBuilder<B extends StreamItemReaderBuilder<B>> {

        protected XReadArgs.StreamOffset<String> offset;
        protected Long count;
        protected boolean noack;

        public B offset(XReadArgs.StreamOffset<String> offset) {
            this.offset = offset;
            return (B) this;
        }

        public B count(long count) {
            this.count = count;
            return (B) this;
        }

        public B noack(boolean noack) {
            this.noack = noack;
            return (B) this;
        }

    }


}
