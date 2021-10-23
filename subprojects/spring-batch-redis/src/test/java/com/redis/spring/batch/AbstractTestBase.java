package com.redis.spring.batch;

import java.util.concurrent.TimeoutException;

import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.job.JobFactory;
import com.redis.testcontainers.RedisServer;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase implements InitializingBean {

	private static final int DEFAULT_CHUNK_SIZE = 50;

	@Autowired
	private JobRepository jobRepository;
	@Autowired
	private PlatformTransactionManager transactionManager;
	protected JobFactory jobFactory;
	protected JobFactory inMemoryJobFactory;

	@Override
	public void afterPropertiesSet() throws Exception {
		jobFactory = new JobFactory(jobRepository, transactionManager);
		inMemoryJobFactory = JobFactory.inMemory();
	}

	protected String name(RedisServer server, String name) {
		if (server.isCluster()) {
			return "cluster-" + name;
		}
		return name;
	}

	protected <T> JobExecution runFlushing(RedisServer redis, String name, PollableItemReader<? extends T> reader,
			ItemWriter<T> writer) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, InterruptedException, TimeoutException {
		return jobFactory.runFlushing(name(redis, name), DEFAULT_CHUNK_SIZE, reader, writer);
	}

	protected <T> JobExecution run(RedisServer redis, String name, ItemReader<? extends T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return jobFactory.run(name(redis, name), DEFAULT_CHUNK_SIZE, reader, writer);
	}

	protected <I, O> JobExecution run(RedisServer redis, String name, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		return jobFactory.run(name(redis, name), DEFAULT_CHUNK_SIZE, reader, processor, writer);
	}

	public <T> SimpleStepBuilder<T, T> step(String name, ItemReader<? extends T> reader, ItemWriter<T> writer) {
		return jobFactory.step(name, DEFAULT_CHUNK_SIZE, reader, writer);
	}

}
