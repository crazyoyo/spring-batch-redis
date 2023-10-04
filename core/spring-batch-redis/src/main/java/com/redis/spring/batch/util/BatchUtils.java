package com.redis.spring.batch.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public abstract class BatchUtils {

    public static final long SIZE_UNKNOWN = -1;

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

    public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
        List<T> list = new ArrayList<>();
        T element;
        while ((element = reader.read()) != null) {
            list.add(element);
        }
        return list;
    }

    public static <S, T> ItemProcessor<S, T> processor(ItemProcessor<?, ?>... processors) {
        return processor(Arrays.asList(processors));
    }

    @SuppressWarnings("unchecked")
    public static <S, T> ItemProcessor<S, T> processor(Collection<? extends ItemProcessor<?, ?>> processors) {
        if (processors.isEmpty()) {
            return null;
        }
        if (processors.size() == 1) {
            return (ItemProcessor<S, T>) processors.iterator().next();
        }
        CompositeItemProcessor<S, T> composite = new CompositeItemProcessor<>();
        composite.setDelegates(new ArrayList<>(processors));
        return composite;
    }

    @SuppressWarnings("unchecked")
    public static <T> ItemWriter<T> writer(ItemWriter<T>... writers) {
        return writer(Arrays.asList(writers));
    }

    public static <T> ItemWriter<T> writer(Collection<? extends ItemWriter<T>> writers) {
        if (writers.isEmpty()) {
            throw new IllegalArgumentException("At least one writer must be specified");
        }
        if (writers.size() == 1) {
            return writers.iterator().next();
        }
        CompositeItemWriter<T> composite = new CompositeItemWriter<>();
        composite.setDelegates(new ArrayList<>(writers));
        return composite;
    }

}
