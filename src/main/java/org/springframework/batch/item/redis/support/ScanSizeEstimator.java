package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class ScanSizeEstimator<C extends StatefulConnection<String, String>> {

    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<String, String>> async;

    public ScanSizeEstimator(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<String, String>> async) {
        Assert.notNull(pool, "A Redis connection pool is required");
        Assert.notNull(async, "An async command function is required");
        this.pool = pool;
        this.async = async;
    }

    @SuppressWarnings("unchecked")
    public long estimate(Options options) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<String, String> commands = this.async.apply(connection);
            commands.setAutoFlushCommands(false);
            RedisFuture<Long> dbsizeFuture = ((RedisServerAsyncCommands<String, String>) commands).dbsize();
            List<RedisFuture<String>> keyFutures = new ArrayList<>(options.getSampleSize());
            // rough estimate of keys matching pattern
            for (int index = 0; index < options.getSampleSize(); index++) {
                keyFutures.add(((RedisKeyAsyncCommands<String, String>) commands).randomkey());
            }
            commands.flushCommands();
            long commandTimeout = connection.getTimeout().toMillis();
            int matchCount = 0;
            Map<String, RedisFuture<String>> keyTypeFutures = new HashMap<>();
            for (RedisFuture<String> future : keyFutures) {
                String key = future.get(commandTimeout, TimeUnit.MILLISECONDS);
                if (key == null) {
                    continue;
                }
                keyTypeFutures.put(key, options.getType() == null ? null : ((RedisKeyAsyncCommands<String, String>) commands).type(key));
            }
            commands.flushCommands();
            Predicate<String> matchPredicate = predicate(options.getMatch());
            for (Map.Entry<String, RedisFuture<String>> entry : keyTypeFutures.entrySet()) {
                if (matchPredicate.test(entry.getKey())) {
                    if (options.getType() == null || options.getType().code().equals(entry.getValue().get(commandTimeout, TimeUnit.MILLISECONDS))) {
                        matchCount++;
                    }
                }
            }
            commands.setAutoFlushCommands(true);
            Long dbsize = dbsizeFuture.get(commandTimeout, TimeUnit.MILLISECONDS);
            if (dbsize == null) {
                return 0;
            }
            return dbsize * matchCount / options.getSampleSize();
        }
    }

    private Predicate<String> predicate(String match) {
        if (match == null) {
            return k -> true;
        }
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(match));
        return k -> pattern.matcher(k).matches();
    }

    @Data
    @Builder
    public static class Options {

        public static final int DEFAULT_SAMPLE_SIZE = 100;

        @Builder.Default
        private int sampleSize = DEFAULT_SAMPLE_SIZE;
        private String match;
        private DataType type;
    }

}
