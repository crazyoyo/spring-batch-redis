package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class RedisClusterDataStructureItemWriters {


    public static class RedisClusterEvalItemWriter<K, V, T> extends DataStructureItemWriterBuilder.EvalItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterEvalItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, String sha, ScriptOutputType outputType, Converter<T, K[]> keysConverter, Converter<T, V[]> argsConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, sha, outputType, keysConverter, argsConverter);
        }

    }

    public static class RedisClusterExpireItemWriter<K, V, T> extends DataStructureItemWriterBuilder.ExpireItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterExpireItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Long> timeoutConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, timeoutConverter);
        }

    }

    public static class RedisClusterGeoSetItemWriter<K, V, T> extends DataStructureItemWriterBuilder.GeoSetItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterGeoSetItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> longitudeConverter, Converter<T, Double> latitudeConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, memberIdConverter, longitudeConverter, latitudeConverter);
        }

    }

    public static class RedisClusterHashItemWriter<K, V, T> extends DataStructureItemWriterBuilder.HashItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterHashItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> mapConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, mapConverter);
        }

    }

    public static class RedisClusterListItemWriter<K, V, T> extends DataStructureItemWriterBuilder.ListItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterListItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, boolean right) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, memberIdConverter, right);
        }

    }

    public static class RedisClusterNoopItemWriter<K, V, T> extends DataStructureItemWriterBuilder.NoopItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterNoopItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout);
        }

    }

    public static class RedisClusterSetItemWriter<K, V, T> extends DataStructureItemWriterBuilder.SetItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterSetItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, memberIdConverter);
        }

    }

    public static class RedisClusterSortedSetItemWriter<K, V, T> extends DataStructureItemWriterBuilder.SortedSetItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterSortedSetItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> scoreConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, memberIdConverter, scoreConverter);
        }

    }

    public static class RedisClusterStreamItemWriter<K, V, T> extends DataStructureItemWriterBuilder.StreamItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterStreamItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter, Converter<T, String> idConverter, Long maxlen, boolean approximateTrimming) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, bodyConverter, idConverter, maxlen, approximateTrimming);
        }

    }

    public static class RedisClusterStringItemWriter<K, V, T> extends DataStructureItemWriterBuilder.StringItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

        public RedisClusterStringItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> valueConverter) {
            super(pool, StatefulRedisClusterConnection::async, commandTimeout, keyConverter, valueConverter);
        }

    }
}

