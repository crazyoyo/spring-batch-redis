package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class RedisDataStructureItemWriters {

    public static class RedisEvalItemWriter<K, V, T> extends DataStructureItemWriterBuilder.EvalItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisEvalItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, String sha, ScriptOutputType outputType, Converter<T, K[]> keysConverter, Converter<T, V[]> argsConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, sha, outputType, keysConverter, argsConverter);
        }

    }

    public static class RedisExpireItemWriter<K, V, T> extends DataStructureItemWriterBuilder.ExpireItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisExpireItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Long> timeoutConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, timeoutConverter);
        }

    }

    public static class RedisGeoSetItemWriter<K, V, T> extends DataStructureItemWriterBuilder.GeoSetItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisGeoSetItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> longitudeConverter, Converter<T, Double> latitudeConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, memberIdConverter, longitudeConverter, latitudeConverter);
        }

    }

    public static class RedisHashItemWriter<K, V, T> extends DataStructureItemWriterBuilder.HashItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisHashItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> mapConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, mapConverter);
        }

    }

    public static class RedisListItemWriter<K, V, T> extends DataStructureItemWriterBuilder.ListItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisListItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, boolean right) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, memberIdConverter, right);
        }

    }

    public static class RedisNoopItemWriter<K, V, T> extends DataStructureItemWriterBuilder.NoopItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisNoopItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout) {
            super(pool, StatefulRedisConnection::async, commandTimeout);
        }


    }

    public static class RedisSetItemWriter<K, V, T> extends DataStructureItemWriterBuilder.SetItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisSetItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, memberIdConverter);
        }

    }

    public static class RedisSortedSetItemWriter<K, V, T> extends DataStructureItemWriterBuilder.SortedSetItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisSortedSetItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> scoreConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, memberIdConverter, scoreConverter);
        }

    }

    public static class RedisStreamItemWriter<K, V, T> extends DataStructureItemWriterBuilder.StreamItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisStreamItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, bodyConverter, null, null, false);
        }

        public RedisStreamItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter, Converter<T, String> idConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, bodyConverter, idConverter, null, false);
        }

        public RedisStreamItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter, Converter<T, String> idConverter, Long maxlen, boolean approximateTrimming) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, bodyConverter, idConverter, maxlen, approximateTrimming);
        }

    }

    public static class RedisStringItemWriter<K, V, T> extends DataStructureItemWriterBuilder.StringItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

        public RedisStringItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, Converter<T, K> keyConverter, Converter<T, V> valueConverter) {
            super(pool, StatefulRedisConnection::async, commandTimeout, keyConverter, valueConverter);
        }

    }

}

