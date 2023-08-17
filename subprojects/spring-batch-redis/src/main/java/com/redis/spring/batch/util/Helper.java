package com.redis.spring.batch.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.FileCopyUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.AbstractRedisItemStreamSupport;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisScriptingCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

/**
 * Helper class for Spring Batch Redis
 * 
 * @author Julien Ruaux
 */
public interface Helper {

    String METRICS_PREFIX = "spring.batch.redis.";

    Boolean NULL_BOOLEAN = null;

    static <T extends Collection<?>> T createGaugeCollectionSize(String name, T collection, Tag... tags) {
        return Metrics.globalRegistry.gaugeCollectionSize(METRICS_PREFIX + name, Arrays.asList(tags), collection);
    }

    static <K, V> Supplier<StatefulConnection<K, V>> connectionSupplier(AbstractRedisClient client, RedisCodec<K, V> codec,
            ReadFrom readFrom) {
        if (client instanceof RedisModulesClusterClient) {
            return () -> {
                StatefulRedisClusterConnection<K, V> connection = ((RedisModulesClusterClient) client).connect(codec);
                if (readFrom != null) {
                    connection.setReadFrom(readFrom);
                }
                return connection;
            };
        }
        return () -> ((RedisModulesClient) client).connect(codec);
    }

    @SuppressWarnings("unchecked")
    static <K, V, T> T sync(StatefulConnection<K, V> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return (T) ((StatefulRedisClusterConnection<K, V>) connection).sync();
        }
        return (T) ((StatefulRedisConnection<K, V>) connection).sync();
    }

    @SuppressWarnings("unchecked")
    static <K, V, T> T async(StatefulConnection<K, V> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return (T) ((StatefulRedisClusterConnection<K, V>) connection).async();
        }
        return (T) ((StatefulRedisConnection<K, V>) connection).async();
    }

    static AsyncTaskExecutor threadPoolTaskExecutor(int threads) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threads);
        taskExecutor.setCorePoolSize(threads);
        taskExecutor.setQueueCapacity(threads);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    public static JobRepository inMemoryJobRepository() throws Exception {
        @SuppressWarnings("deprecation")
        org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
        bean.afterPropertiesSet();
        return bean.getObject();
    }

    @SuppressWarnings("unchecked")
    static String loadScript(AbstractRedisClient client, String filename) {
        byte[] bytes;
        try (InputStream inputStream = KeyValueItemProcessor.class.getClassLoader().getResourceAsStream(filename)) {
            bytes = FileCopyUtils.copyToByteArray(inputStream);
        } catch (IOException e) {
            throw new ItemStreamException("Could not read LUA script file " + filename);
        }
        try (StatefulConnection<String, String> connection = RedisModulesUtils.connection(client)) {
            return ((RedisScriptingCommands<String, String>) Helper.sync(connection)).scriptLoad(bytes);
        }
    }

    static long getItemReaderSize(ItemReader<?> reader) {
        if (reader instanceof RedisItemReader) {
            RedisItemReader<?, ?> redisItemReader = (RedisItemReader<?, ?>) reader;
            ScanSizeEstimator estimator = new ScanSizeEstimator(redisItemReader.getClient());
            estimator.setScanMatch(redisItemReader.getScanMatch());
            estimator.setScanType(redisItemReader.getScanType());
            return estimator.getAsLong();
        }
        if (reader instanceof KeyComparisonItemReader) {
            return getItemReaderSize(((KeyComparisonItemReader) reader).getLeft());
        }
        if (reader instanceof StreamItemReader) {
            StreamItemReader<?, ?> streamItemReader = (StreamItemReader<?, ?>) reader;
            return streamItemReader.streamLength();
        }
        if (reader instanceof GeneratorItemReader) {
            return ((GeneratorItemReader) reader).size();
        }
        return -1;
    }

    static boolean isOpen(Object object) {
        return isOpen(object, true);
    }

    static boolean isOpen(Object object, boolean defaultValue) {
        Boolean value = isNullableOpen(object);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @SuppressWarnings("rawtypes")
    static Boolean isNullableOpen(Object object) {
        if (object instanceof GeneratorItemReader) {
            return ((GeneratorItemReader) object).isOpen();
        }
        if (object instanceof RedisItemReader) {
            return ((RedisItemReader) object).isOpen();
        }
        if (object instanceof KeyspaceNotificationItemReader) {
            return ((KeyspaceNotificationItemReader) object).isOpen();
        }
        if (object instanceof StreamItemReader) {
            return ((StreamItemReader) object).isOpen();
        }
        if (object instanceof AbstractRedisItemStreamSupport) {
            return ((AbstractRedisItemStreamSupport) object).isOpen();
        }
        if (object instanceof KeyspaceNotificationItemReader) {
            return ((KeyspaceNotificationItemReader) object).isOpen();
        }
        if (object instanceof KeyComparisonItemReader) {
            return ((KeyComparisonItemReader) object).isOpen();
        }
        return NULL_BOOLEAN;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <K> Function<String, K> stringKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return key -> codec.decodeKey(StringCodec.UTF8.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <K> Function<K, String> toStringKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return key -> StringCodec.UTF8.decodeKey(codec.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <V> Function<String, V> stringValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return value -> codec.decodeValue(StringCodec.UTF8.encodeValue(value));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <V> Function<V, String> toStringValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return value -> StringCodec.UTF8.decodeValue(codec.encodeValue(value));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <K> Function<byte[], K> byteArrayKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return key -> codec.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <K> Function<K, byte[]> toByteArrayKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return key -> ByteArrayCodec.INSTANCE.decodeKey(codec.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <V> Function<byte[], V> byteArrayValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return value -> codec.decodeValue(ByteArrayCodec.INSTANCE.encodeValue(value));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <V> Function<V, byte[]> toByteArrayValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return value -> ByteArrayCodec.INSTANCE.decodeValue(codec.encodeValue(value));
    }

}
