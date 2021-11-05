package com.redis.spring.batch.builder;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.PlatformTransactionManager;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class JobRepositoryBuilder<K, V, B extends JobRepositoryBuilder<K, V, B>> extends RedisBuilder<K, V, B> {

	protected JobRepository jobRepository;
	protected PlatformTransactionManager transactionManager;

	public JobRepositoryBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}

	@SuppressWarnings("unchecked")
	public B jobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B transactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
		return (B) this;
	}

	public void inMemory() throws Exception {
		@SuppressWarnings("deprecation")
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		bean.afterPropertiesSet();
		jobRepository = bean.getObject();
		transactionManager = bean.getTransactionManager();
	}
}
