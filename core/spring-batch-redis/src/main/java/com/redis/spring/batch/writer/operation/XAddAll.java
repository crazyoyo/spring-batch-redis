package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class XAddAll<K, V, T> implements Operation<K, V, T, Object> {

    private Function<T, Collection<StreamMessage<K, V>>> messages;

    private Function<StreamMessage<K, V>, XAddArgs> args = m -> new XAddArgs().id(m.getId());

    public void setMessages(Function<T, Collection<StreamMessage<K, V>>> messages) {
        this.messages = messages;
    }

    public void setArgs(Function<StreamMessage<K, V>, XAddArgs> args) {
        this.args = args;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
        for (StreamMessage<K, V> message : messages(item)) {
            futures.add((RedisFuture) streamCommands.xadd(message.getStream(), args(message), message.getBody()));
        }
    }

    private XAddArgs args(StreamMessage<K, V> message) {
        return args.apply(message);
    }

    private Collection<StreamMessage<K, V>> messages(T item) {
        return messages.apply(item);
    }

}
