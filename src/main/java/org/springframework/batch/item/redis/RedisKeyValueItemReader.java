package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemProcessor;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.RedisItemReader;

import java.util.List;

public class RedisKeyValueItemReader<K> extends RedisItemReader<K, KeyValue<K>> {

    public RedisKeyValueItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<KeyValue<K>>> valueProcessor, ReaderOptions options) {
        super(keyReader, valueProcessor, options);
    }

    public static RedisKeyValueItemReaderBuilder builder() {
        return new RedisKeyValueItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyValueItemReaderBuilder extends RedisItemReaderBuilder {

        private RedisOptions redisOptions = RedisOptions.builder().build();
        private ReaderOptions readerOptions = ReaderOptions.builder().build();

        public RedisKeyValueItemReader<String> build() {
            return new RedisKeyValueItemReader<>(keyReader(redisOptions, readerOptions), new KeyValueItemProcessor<>(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout()), readerOptions);
        }
    }
}
