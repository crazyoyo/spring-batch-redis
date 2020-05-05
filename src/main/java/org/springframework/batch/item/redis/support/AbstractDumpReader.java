package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public abstract class AbstractDumpReader<K, V> implements ItemProcessor<List<K>, List<KeyDump<K>>> {

    @Getter
    @Setter
    private long timeout;

    protected AbstractDumpReader(Duration timeout) {
        this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout.getSeconds();
    }

    protected List<KeyDump<K>> read(List<K> keys, BaseRedisAsyncCommands<K, V> commands) {
        commands.setAutoFlushCommands(false);
        List<RedisFuture<Long>> ttls = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumps = new ArrayList<>(keys.size());
        for (K key : keys) {
            ttls.add(((RedisKeyAsyncCommands<K, V>) commands).pttl(key));
            dumps.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyDump<K>> keyDumps = new ArrayList<>();
        for (int index = 0; index < keys.size(); index++) {
            try {
                Long ttl = ttls.get(index).get(timeout, TimeUnit.SECONDS);
                byte[] dump = dumps.get(index).get(timeout, TimeUnit.SECONDS);
                keyDumps.add(new KeyDump<>(keys.get(index), ttl, dump));
            } catch (InterruptedException e) {
                log.debug("Interrupted while dumping", e);
            } catch (ExecutionException e) {
                log.error("Could not dump", e);
            } catch (TimeoutException e) {
                log.error("Timeout in DUMP command", e);
            }
        }
        commands.setAutoFlushCommands(true);
        return keyDumps;
    }

}
