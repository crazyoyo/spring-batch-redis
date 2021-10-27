package com.redis.spring.batch;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.State;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.testcontainers.RedisServer;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase {

	private static final int DEFAULT_CHUNK_SIZE = 50;
	protected static final Duration IDLE_TIMEOUT = Duration.ofSeconds(6);

	@Autowired
	protected JobRepository jobRepository;
	@Autowired
	protected PlatformTransactionManager transactionManager;
	@Autowired
	protected JobBuilderFactory jobBuilderFactory;
	@Autowired
	protected StepBuilderFactory stepBuilderFactory;
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobLauncher asyncJobLauncher;

	protected String name(RedisServer redis, String name) {
		return redis.getRedisURI() + "-" + name;
	}

	protected <I, O> JobExecution run(Job job) throws JobExecutionException {
		return awaitTermination(jobLauncher.run(job, new JobParameters()));
	}

	protected JobExecution awaitTermination(JobExecution execution) {
		Awaitility.await().timeout(Duration.ofSeconds(60)).until(() -> !execution.isRunning());
		return execution;
	}

	protected FlowBuilder<SimpleFlow> flow(RedisServer redis, String name) {
		return new FlowBuilder<SimpleFlow>(name(redis, name));
	}

	protected <I, O> SimpleStepBuilder<I, O> step(RedisServer redis, String name, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		return stepBuilderFactory.get(name(redis, name)).<I, O>chunk(DEFAULT_CHUNK_SIZE).reader(reader)
				.processor(processor).writer(writer);
	}

	protected SimpleJobBuilder job(RedisServer redis, String name, Step step) {
		return job(redis, name).start(step);
	}

	protected JobBuilder job(RedisServer redis, String name) {
		return jobBuilderFactory.get(name(redis, name));
	}

	protected <I, O> JobExecution run(RedisServer redis, String name, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		return run(job(redis, name, step(redis, name, reader, processor, writer).build()).build());
	}

	protected <T> JobExecution run(RedisServer redis, String name, ItemReader<? extends T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return run(redis, name, reader, null, writer);
	}

	protected <I, O> JobExecution runFlushing(RedisServer redis, String name, PollableItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		FlushingStepBuilder<I, O> step = flushingStep(redis, name, reader, processor, writer);
		Job job = job(redis, name, step.build()).build();
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		awaitRunning(execution);
		awaitOpen(reader);
		return execution;
	}

	private JobExecution awaitRunning(JobExecution execution) {
		Awaitility.await().until(() -> execution.isRunning());
		return execution;
	}

	protected void awaitOpen(PollableItemReader<?> reader) {
		Awaitility.await().until(() -> reader.getState() == State.OPEN);
	}

	protected <I, O> FlushingStepBuilder<I, O> flushingStep(RedisServer redis, String name,
			PollableItemReader<? extends I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionException {
		return new FlushingStepBuilder<>(step(redis, name, reader, processor, writer)).idleTimeout(IDLE_TIMEOUT);
	}

	protected void execute(GeneratorBuilder generator) throws Exception {
		awaitTermination(generator.build().call());
	}

	protected JobExecution runAsync(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return awaitRunning(asyncJobLauncher.run(job, new JobParameters()));
	}

}
