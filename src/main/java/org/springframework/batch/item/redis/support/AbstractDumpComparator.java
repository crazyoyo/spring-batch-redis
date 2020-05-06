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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public abstract class AbstractDumpComparator<K, V> implements ItemProcessor<List<K>, List<KeyComparison<K>>> {

    private final static long DEFAULT_PTTL_TOLERANCE = 5;

    @Getter
    @Setter
    private ItemProcessor<List<K>, List<KeyDump<K>>> reader;
    @Getter
    @Setter
    private long timeout;
    @Getter
    @Setter
    private long pttlTolerance;

    protected AbstractDumpComparator(ItemProcessor<List<K>, List<KeyDump<K>>> reader, Duration timeout, Long pttlTolerance) {
        this.reader = reader;
        this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout.getSeconds();
        this.pttlTolerance = pttlTolerance == null ? DEFAULT_PTTL_TOLERANCE : pttlTolerance;
    }

    protected List<KeyComparison<K>> compare(List<K> keys, BaseRedisAsyncCommands<K, V> commands) throws Exception {
        List<KeyComparison<K>> comparisons = new ArrayList<>();
        List<KeyDump<K>> sourceDumps = reader.process(keys);
        if (sourceDumps == null) {
            return comparisons;
        }
        commands.setAutoFlushCommands(false);
        List<RedisFuture<Long>> ttls = new ArrayList<>(sourceDumps.size());
        List<RedisFuture<byte[]>> dumps = new ArrayList<>(sourceDumps.size());
        for (KeyDump<K> source : sourceDumps) {
            ttls.add(((RedisKeyAsyncCommands<K, V>) commands).pttl(source.getKey()));
            dumps.add(((RedisKeyAsyncCommands<K, V>) commands).dump(source.getKey()));
        }
        commands.flushCommands();
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
