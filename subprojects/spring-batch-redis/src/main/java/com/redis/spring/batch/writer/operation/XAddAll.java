package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class XAddAll<K, V, T> implements WriteOperation<K, V, T> {

    private final Function<T, Collection<StreamMessage<K, V>>> messagesFunction;

    private final Xadd<K, V, StreamMessage<K, V>> xadd;

    public XAddAll(Function<T, Collection<StreamMessage<K, V>>> messages, Function<StreamMessage<K, V>, XAddArgs> args) {
        this.messagesFunction = messages;
        this.xadd = new Xadd<>(StreamMessage::getStream, StreamMessage::getBody, args);
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        for (StreamMessage<K, V> message : messagesFunction.apply(item)) {
            xadd.execute(commands, message, futures);
        }
    }

}
