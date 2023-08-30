package com.redis.spring.batch.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.AbstractRedisItemStreamSupport;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;

public abstract class BatchUtils {

    private static final Boolean NULL_BOOLEAN = null;

    private BatchUtils() {
    }

    public static AsyncTaskExecutor threadPoolTaskExecutor(int threads) {
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

    public static long size(ItemReader<?> reader) {
        if (reader instanceof RedisItemReader) {
            RedisItemReader<?, ?> redisItemReader = (RedisItemReader<?, ?>) reader;
            ScanSizeEstimator estimator = new ScanSizeEstimator(redisItemReader.getClient());
            estimator.setScanMatch(redisItemReader.getScanMatch());
            estimator.setScanType(redisItemReader.getScanType());
            return estimator.getAsLong();
        }
        if (reader instanceof AbstractCountingItemReader) {
            return ((AbstractCountingItemReader<?>) reader).size();
        }
        if (reader instanceof KeyComparisonItemReader) {
            return size(((KeyComparisonItemReader) reader).getLeft());
        }
        if (reader instanceof StreamItemReader) {
            StreamItemReader<?, ?> streamItemReader = (StreamItemReader<?, ?>) reader;
            return streamItemReader.streamLength();
        }
        return -1;
    }

    public static boolean isOpen(Object object) {
        return isOpen(object, true);
    }

    public static boolean isClosed(Object object) {
        return !isOpen(object, false);
    }

    private static boolean isOpen(Object object, boolean defaultValue) {
        Boolean value = isNullableOpen(object);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @SuppressWarnings("rawtypes")
    private static Boolean isNullableOpen(Object object) {
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

    public static boolean isPositive(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }

    public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
        List<T> list = new ArrayList<>();
        T element;
        while ((element = reader.read()) != null) {
            list.add(element);
        }
        return list;
    }

}
