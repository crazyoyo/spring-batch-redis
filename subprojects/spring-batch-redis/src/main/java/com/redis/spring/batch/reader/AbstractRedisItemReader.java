package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.CompositeItemStreamProcessor;
import com.redis.spring.batch.common.ItemStreamProcessor;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.ListItemProcessor;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.QueueItemWriter;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.common.ValueType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractRedisItemReader<K, V> extends AbstractItemStreamItemReader<KeyValue<K>> {

	protected final AbstractRedisClient client;
	protected final RedisCodec<K, V> codec;
	protected final KeyItemReader<K> keyReader;
	private final ValueType valueType;
	private ItemProcessor<K, K> processor;
	private JobRepository jobRepository;
	private JobBuilderFactory jobBuilderFactory;
	protected ReaderOptions options = ReaderOptions.builder().build();
	private String name;
	private JobExecution jobExecution;
	protected BlockingQueue<KeyValue<K>> queue;

	protected AbstractRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, KeyItemReader<K> keyReader,
			ValueType valueType) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.keyReader = keyReader;
		this.valueType = valueType;
	}

	public KeyItemReader<K> getKeyReader() {
		return keyReader;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public ValueType getValueType() {
		return valueType;
	}

	public ReaderOptions getOptions() {
		return options;
	}

	public void setOptions(ReaderOptions options) {
		this.options = options;
	}

	public JobRepository getJobRepository() {
		return jobRepository;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setKeyProcessor(ItemProcessor<K, K> processor) {
		this.processor = processor;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (!isOpen()) {
			doOpen();
		}
	}

	protected void doOpen() {
		try {
			jobExecution = jobLauncher().run(job(), new JobParameters());
		} catch (JobExecutionException e) {
			throw new ItemStreamException("Job execution failed", e);
		}
		while (!(Utils.isOpen(keyReader) || jobExecution.getStatus().isUnsuccessful()
				|| jobExecution.getStatus().isLessThanOrEqualTo(BatchStatus.COMPLETED))) {
			sleep();
		}
		if (jobExecution.getStatus().isUnsuccessful()) {
			throw new ItemStreamException("Could not run job",
					jobExecution.getAllFailureExceptions().iterator().next());
		}
	}

	private Job job() {
		return jobBuilderFactory().get(name).start(step().build()).build();
	}

	private JobBuilderFactory jobBuilderFactory() {
		if (jobBuilderFactory == null) {
			jobBuilderFactory = new JobBuilderFactory(jobRepository());
		}
		return jobBuilderFactory;
	}

	private JobRepository jobRepository() {
		if (jobRepository == null) {
			try {
				jobRepository = Utils.inMemoryJobRepository();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job repository", e);
			}
		}
		return jobRepository;
	}

	private SimpleJobLauncher jobLauncher() {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository());
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		return jobLauncher;
	}

	private void sleep() {
		try {
			Thread.sleep(options.getQueueOptions().getPollTimeout().toMillis());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ItemStreamException("Interrupted during initialization", e);
		}
	}

	private PlatformTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected SimpleStepBuilder<K, K> step() {
		StepBuilder stepBuilder = new StepBuilder(name);
		stepBuilder.repository(jobRepository());
		stepBuilder.transactionManager(transactionManager());
		SimpleStepBuilder<K, K> step = stepBuilder.chunk(options.getChunkSize());
		step.reader(keyReader);
		step.processor(processor);
		step.writer(new ProcessingItemWriter<>((ItemProcessor) operationProcessor(), queueWriter()));
		Utils.multiThread(step, options.getThreads());
		return step;
	}

	protected ItemWriter<KeyValue<K>> queueWriter() {
		queue = new LinkedBlockingQueue<>(options.getQueueOptions().getCapacity());
		Utils.createGaugeCollectionSize("reader.queue.size", queue);
		return new QueueItemWriter<>(queue);
	}

	public ItemStreamProcessor<List<K>, List<KeyValue<K>>> operationProcessor() {
		KeyValueReadOperation<K, V> op = new KeyValueReadOperation<>(client, codec);
		op.setMemoryUsageOptions(options.getMemoryUsageOptions());
		op.setValueType(valueType);
		OperationItemProcessor<K, V, K, List<Object>> opProc = new OperationItemProcessor<>(client, codec, op);
		opProc.setPoolOptions(options.getPoolOptions());
		opProc.setReadFrom(options.getReadFrom());
		return new CompositeItemStreamProcessor<>(opProc, keyValueListProcessor());
	}

	private ItemProcessor<List<List<Object>>, List<KeyValue<K>>> keyValueListProcessor() {
		KeyValueProcessor<K, V> kvProc = new KeyValueProcessor<>(codec);
		ItemProcessor<List<List<Object>>, List<KeyValue<K>>> listProc = new ListItemProcessor<>(kvProc);
		if (valueType == ValueType.STRUCT) {
			return new CompositeItemStreamProcessor<>(listProc, new ListItemProcessor<>(new StructProcessor<>(codec)));
		}
		return listProc;
	}

	@Override
	public synchronized void close() {
		if (isOpen()) {
			doClose();
		}
		super.close();
	}

	protected void doClose() {
		queue = null;
		if (jobExecution.isRunning()) {
			for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
				stepExecution.setTerminateOnly();
			}
			jobExecution.setStatus(BatchStatus.STOPPING);
		}
		jobExecution = null;
	}

	public boolean isOpen() {
		return jobExecution != null;
	}

	@Override
	public synchronized KeyValue<K> read() throws Exception {
		KeyValue<K> item;
		do {
			item = queue.poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution != null && jobExecution.isRunning());
		if (jobExecution != null && jobExecution.getStatus().isUnsuccessful()) {
			throw new ItemStreamException("Reader job failed");
		}
		return item;
	}

	public synchronized List<KeyValue<K>> read(int maxElements) {
		List<KeyValue<K>> items = new ArrayList<>(maxElements);
		queue.drainTo(items, maxElements);
		return items;
	}

}
