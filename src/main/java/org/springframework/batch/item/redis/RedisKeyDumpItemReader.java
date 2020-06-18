package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.KeyDumpItemProcessor;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.RedisItemReader;

import java.util.List;

public class RedisKeyDumpItemReader<K> extends RedisItemReader<K, KeyDump<K>> {

    public RedisKeyDumpItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<KeyDump<K>>> valueProcessor, ReaderOptions options) {
        super(keyReader, valueProcessor, options);
    }

    public static RedisKeyDumpItemReaderBuilder builder() {
        return new RedisKeyDumpItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyDumpItemReaderBuilder extends RedisItemReaderBuilder {

        private RedisOptions redisOptions = RedisOptions.builder().build();
        private ReaderOptions readerOptions = ReaderOptions.builder().build();

        public RedisKeyDumpItemReader<String> build() {
            return new RedisKeyDumpItemReader<>(keyReader(redisOptions, readerOptions), new KeyDumpItemProcessor<>(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout()), readerOptions);
        }
    }
}
