package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public class KeyDumpComparator<K, V, C extends StatefulConnection<K, V>> implements ItemProcessor<List<? extends K>, List<? extends KeyComparison<K>>> {

    @NonNull
    private final ItemProcessor<List<? extends K>, List<? extends KeyDump<K>>> reader;
    @NonNull
    private final GenericObjectPool<C> pool;
    @NonNull
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final long timeout;
    private final long pttlTolerance;

    @Builder
    public KeyDumpComparator(ItemProcessor<List<? extends K>, List<? extends KeyDump<K>>> reader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout, long pttlTolerance) {
        Assert.isTrue(commandTimeout > 0, "Command timeout must be positive.");
        this.reader = reader;
        this.pool = pool;
        this.commands = commands;
        this.timeout = commandTimeout;
        this.pttlTolerance = pttlTolerance;
    }

    @Override
    public List<KeyComparison<K>> process(List<? extends K> keys) throws Exception {
        List<? extends KeyDump<K>> sourceDumps = reader.process(keys);
        if (sourceDumps == null) {
            return Collections.emptyList();
        }
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<Long>> ttls = new ArrayList<>(sourceDumps.size());
            List<RedisFuture<byte[]>> dumps = new ArrayList<>(sourceDumps.size());
            for (KeyDump<K> source : sourceDumps) {
                ttls.add(((RedisKeyAsyncCommands<K, V>) commands).pttl(source.getKey()));
                dumps.add(((RedisKeyAsyncCommands<K, V>) commands).dump(source.getKey()));
            }
            commands.flushCommands();
            List<KeyComparison<K>> comparisons = new ArrayList<>();
            for (int index = 0; index < sourceDumps.size(); index++) {
                try {
                    Long ttl = ttls.get(index).get(timeout, TimeUnit.SECONDS);
                    byte[] dump = dumps.get(index).get(timeout, TimeUnit.SECONDS);
                    KeyDump<K> source = sourceDumps.get(index);
                    KeyDump<K> target = new KeyDump<>(source.getKey(), ttl, dump);
                    comparisons.add(new KeyComparison<>(source, target, getStatus(source, target)));
                } catch (InterruptedException e) {
                    log.debug("Interrupted while dumping", e);
                } catch (ExecutionException e) {
                    log.error("Could not dump", e);
                } catch (TimeoutException e) {
                    log.error("Timeout in DUMP command", e);
                }
            }
            commands.setAutoFlushCommands(true);
            return comparisons;
        }
    }

    public KeyComparison.Status getStatus(KeyDump<K> source, KeyDump<K> target) {
        if (source.getValue() == null) {
            if (target.getValue() == null) {
                return KeyComparison.Status.OK;
            }
            return KeyComparison.Status.EXTRA;
        }
        if (target.getValue() == null) {
            return KeyComparison.Status.MISSING;
        }
        if (Math.abs(source.getPttl() - target.getPttl()) > pttlTolerance) {
            return KeyComparison.Status.TTL;
        }
        if (Arrays.equals(source.getValue(), target.getValue())) {
            return KeyComparison.Status.OK;
        }
        return KeyComparison.Status.MISMATCH;
    }

}
