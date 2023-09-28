package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.writer.BatchWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class XAddAll<K, V, T> implements BatchWriteOperation<K, V, T> {

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
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        if (CollectionUtils.isEmpty(items)) {
            return Collections.emptyList();
        }
        List<RedisFuture<Object>> futures = new ArrayList<>();
        RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
        for (T item : items) {
            Collection<StreamMessage<K, V>> messages = messagesFunction.apply(item);
            if (CollectionUtils.isEmpty(messages)) {
                continue;
            }
            for (StreamMessage<K, V> message : messages) {
                XAddArgs args = argsFunction.apply(message);
                futures.add((RedisFuture) streamCommands.xadd(message.getStream(), args, message.getBody()));
            }
        }
        return futures;
    }

}
