package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class RedisClusterDatasetSizeEstimator extends AbstractDatasetSizeEstimator {

    private final StatefulRedisClusterConnection<String, String> connection;

    public RedisClusterDatasetSizeEstimator(StatefulRedisClusterConnection<String, String> connection, long commandTimeout, int sampleSize, String keyPattern) {
        super(commandTimeout, sampleSize, keyPattern);
        this.connection = connection;
    }

    @Override
    protected BaseRedisAsyncCommands<String, String> async() {
        return connection.async();
    }

    public static RedisClusterDatasetSizeEstimatorBuilder builder(StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterDatasetSizeEstimatorBuilder(connection);
    }

    public static class RedisClusterDatasetSizeEstimatorBuilder extends DatasetSizeEstimatorBuilder<RedisClusterDatasetSizeEstimatorBuilder> {

        private final StatefulRedisClusterConnection<String, String> connection;

        public RedisClusterDatasetSizeEstimatorBuilder(StatefulRedisClusterConnection<String, String> connection) {
            this.connection = connection;
        }

        public RedisClusterDatasetSizeEstimator build() {
            return new RedisClusterDatasetSizeEstimator(connection, commandTimeout, sampleSize, keyPattern);
        }

    }
}
