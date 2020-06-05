package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class KeyDumpItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractItemReader<K, V, C, KeyValue<K, byte[]>> {

    public KeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, ReaderOptions options) {
        super(keyReader, pool, commands, options);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<KeyValue<K, byte[]>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyValue<K, byte[]>> dumps = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            KeyValue<K, byte[]> dump = new KeyValue<>();
            dump.setKey(keys.get(index));
            try {
                dump.setTtl(getTtl(ttlFutures.get(index)));
                dump.setValue(get(dumpFutures.get(index)));
            } catch (Exception e) {
                log.error("Could not get value", e);
            }
            dumps.add(dump);
        }
        return dumps;
    }

}
