package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@SpringBootApplication
@EnableBatchProcessing
public class BatchTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchTestApplication.class, args);
    }

    @Bean
    RedisURI redisURI() {
        return RedisURI.create("localhost", BaseTest.REDIS_PORT);
    }

    @Bean
    RedisURI targetRedisURI() {
        return RedisURI.create("localhost", BaseTest.TARGET_REDIS_PORT);
    }

    @Bean
    RedisOptions redisOptions(RedisURI redisURI) {
        return RedisOptions.builder().redisURI(redisURI).build();
    }

    @Bean
    RedisOptions targetRedisOptions(RedisURI targetRedisURI) {
        return RedisOptions.builder().redisURI(targetRedisURI).build();
    }

    @Bean
    RedisClient redisClient(RedisURI redisURI) {
        return RedisClient.create(redisURI);
    }

    @Bean
    RedisClient targetRedisClient(RedisURI targetRedisURI) {
        return RedisClient.create(targetRedisURI);
    }

    @Bean
    StatefulRedisConnection<String, String> connection(RedisClient redisClient) {
        return redisClient.connect();
    }

    @Bean
    StatefulRedisConnection<String, String> targetConnection(RedisClient targetRedisClient) {
        return targetRedisClient.connect();
    }

    @Bean
    GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig() {
        return new GenericObjectPoolConfig<>();
    }

    @Bean
    GenericObjectPool<StatefulRedisConnection<String, String>> pool(RedisClient redisClient, GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig) {
        return ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig);
    }

    @Bean
    GenericObjectPool<StatefulRedisConnection<String, String>> targetPool(RedisClient targetRedisClient, GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig) {
        return ConnectionPoolSupport.createGenericObjectPool(targetRedisClient::connect, poolConfig);
    }

    @Bean
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }


}