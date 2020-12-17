package org.springframework.batch.item.redis.support;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public abstract class AbstractStreamItemReader<K, V> extends AbstractPollableItemReader<StreamMessage<K, V>> {

    private final Long count;
    private boolean noack;
    private StreamOffset<K> offset;
    private Iterator<StreamMessage<K, V>> iterator;
    private boolean stopped;

    public AbstractStreamItemReader(StreamOffset<K> offset, Duration block, Long count, boolean noack) {
        super(block);
        Assert.notNull(offset, "Offset is required.");
        this.offset = offset;
        this.count = count;
        this.noack = noack;
    }

    @Override
    protected void doOpen() {
        iterator = new ArrayList<StreamMessage<K, V>>().iterator();
    }

    @Override
    protected void doClose() {
        iterator = null;
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    public boolean isTerminated() {
        return stopped;
    }

    @Override
    public StreamMessage<K, V> poll(Duration timeout) {
        if (!iterator.hasNext()) {
            XReadArgs args = XReadArgs.Builder.block(timeout).noack(noack);
            if (count != null) {
                args.count(count);
            }
            log.debug("Reading stream with args {} and offset {}", args, offset);
            List<StreamMessage<K, V>> messages = commands().xread(args, offset);
            if (messages == null || messages.isEmpty()) {
                return null;
            }
            iterator = messages.iterator();
        }
        StreamMessage<K, V> message = iterator.next();
        offset = StreamOffset.from(message.getStream(), message.getId());
        return message;
    }

    protected abstract RedisStreamCommands<K, V> commands();

}
