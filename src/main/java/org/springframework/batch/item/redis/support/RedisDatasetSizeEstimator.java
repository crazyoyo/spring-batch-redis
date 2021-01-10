package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class RedisDatasetSizeEstimator extends AbstractDatasetSizeEstimator {

    private final StatefulRedisConnection<String, String> connection;

    public RedisDatasetSizeEstimator(StatefulRedisConnection<String, String> connection, long commandTimeout, int sampleSize, String keyPattern) {
        super(commandTimeout, sampleSize, keyPattern);
        this.connection = connection;
    }

    @Override
    protected BaseRedisAsyncCommands<String, String> async() {
        return connection.async();
    }

    public static RedisDatasetSizeEstimatorBuilder builder(StatefulRedisConnection<String, String> connection) {
        return new RedisDatasetSizeEstimatorBuilder(connection);
    }

    public static class RedisDatasetSizeEstimatorBuilder extends DatasetSizeEstimatorBuilder<RedisDatasetSizeEstimatorBuilder> {

        private final StatefulRedisConnection<String, String> connection;

        public RedisDatasetSizeEstimatorBuilder(StatefulRedisConnection<String, String> connection) {
            this.connection = connection;
        }

        public RedisDatasetSizeEstimator build() {
            return new RedisDatasetSizeEstimator(connection, commandTimeout, sampleSize, keyPattern);
        }

    }
}
