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

    private Function<T, Collection<StreamMessage<K, V>>> messagesFunction;

    private Function<StreamMessage<K, V>, XAddArgs> argsFunction = m -> new XAddArgs().id(m.getId());

    public void setMessagesFunction(Function<T, Collection<StreamMessage<K, V>>> function) {
        this.messagesFunction = function;
    }

    public void setArgs(XAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<StreamMessage<K, V>, XAddArgs> function) {
        this.argsFunction = function;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
        for (StreamMessage<K, V> message : messagesFunction.apply(item)) {
            XAddArgs args = argsFunction.apply(message);
            futures.add((RedisFuture) streamCommands.xadd(message.getStream(), args, message.getBody()));
        }
    }

}
