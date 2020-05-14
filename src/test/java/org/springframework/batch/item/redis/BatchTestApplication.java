package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
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

    @Bean(destroyMethod = "shutdown")
    RedisClient client(RedisURI redisURI) {
        return RedisClient.create(redisURI);
    }

    @Bean(destroyMethod = "shutdown")
    RedisClient targetClient(RedisURI targetRedisURI) {
        return RedisClient.create(targetRedisURI);
    }

    @Bean(destroyMethod = "close")
    StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
        return client.connect();
    }

    @Bean(destroyMethod = "close")
    StatefulRedisConnection<String, String> targetRedisConnection(RedisClient targetClient) {
        return targetClient.connect();
    }

    @Bean(destroyMethod = "close")
    GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool(RedisClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>());
    }

    @Bean(destroyMethod = "close")
    GenericObjectPool<StatefulRedisConnection<String, String>> targetConnectionPool(RedisClient targetClient) {
        return ConnectionPoolSupport.createGenericObjectPool(targetClient::connect, new GenericObjectPoolConfig<>());
    }

    @Bean
    RedisCommands<String, String> syncCommands(StatefulRedisConnection<String, String> redisConnection) {
        return redisConnection.sync();
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