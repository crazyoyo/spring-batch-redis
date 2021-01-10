package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import lombok.AllArgsConstructor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

@AllArgsConstructor
public abstract class AbstractDatasetSizeEstimator implements Callable<Long> {

    protected static final int DEFAULT_SAMPLE_SIZE = 100;

    private final long commandTimeout;
    private final int sampleSize;
    private final String keyPattern;

    protected abstract BaseRedisAsyncCommands<String, String> async();

    @Override
    public Long call() throws InterruptedException, ExecutionException, TimeoutException {
        BaseRedisAsyncCommands<String, String> async = async();
        async.setAutoFlushCommands(false);
        RedisFuture<Long> dbsizeFuture = ((RedisServerAsyncCommands<String, String>) async).dbsize();
        List<RedisFuture<String>> keyFutures = new ArrayList<>(sampleSize);
        // rough estimate of keys matching pattern
        for (int index = 0; index < sampleSize; index++) {
            keyFutures.add(((RedisKeyAsyncCommands<String, String>) async).randomkey());
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        int matchCount = 0;
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(keyPattern));
        for (RedisFuture<String> future : keyFutures) {
            String key = future.get(commandTimeout, TimeUnit.SECONDS);
            if (key == null) {
                continue;
            }
            if (pattern.matcher(key).matches()) {
                matchCount++;
            }
        }
        Long dbsize = dbsizeFuture.get(commandTimeout, TimeUnit.SECONDS);
        if (dbsize == null) {
            return null;
        }
        return dbsize * matchCount / sampleSize;
    }

    public static class DatasetSizeEstimatorBuilder<B extends DatasetSizeEstimatorBuilder<B>> {

        protected long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
        protected int sampleSize = DEFAULT_SAMPLE_SIZE;
        protected String keyPattern = KeyValueItemReaderBuilder.DEFAULT_KEY_PATTERN;

        public B commandTimeout(Duration commandTimeout) {
            this.commandTimeout = commandTimeout.getSeconds();
            return (B) this;
        }

        public B commandTimeout(long commandTimeout) {
            this.commandTimeout = commandTimeout;
            return (B) this;
        }

        public B sampleSize(int sampleSize) {
            this.sampleSize = sampleSize;
            return (B) this;
        }

        public B keyPattern(String keyPattern) {
            this.keyPattern = keyPattern;
            return (B) this;
        }

    }
}
