package com.redis.spring.batch.support;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.PlatformTransactionManager;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class JobRepositoryBuilder<K, V, B extends JobRepositoryBuilder<K, V, B>> extends RedisConnectionBuilder<K, V, B> {

	protected JobRepository jobRepository;
	protected PlatformTransactionManager transactionManager;

	public JobRepositoryBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}
	
	public JobRepository getJobRepository() {
		return jobRepository;
	}
	
	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
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

	@SuppressWarnings("unchecked")
	public B inMemoryJobs() throws Exception {
		@SuppressWarnings("deprecation")
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		bean.afterPropertiesSet();
		jobRepository = bean.getObject();
		transactionManager = bean.getTransactionManager();
		return (B) this;
	}
}
