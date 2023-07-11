package com.redis.spring.batch.writer;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.OperationItemProcessor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractRedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private WriterOptions options = WriterOptions.builder().build();
	private OperationItemProcessor<K, V, T, Object> operationProcessor;

	protected AbstractRedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
	}

	public WriterOptions getOptions() {
		return options;
	}

	public void setOptions(WriterOptions options) {
		this.options = options;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (operationProcessor == null) {
			BatchOperation<K, V, T, Object> operation = operation();
			if (options.getReplicaWaitOptions().getReplicas() > 0) {
				ReplicaWaitWriteOperation<K, V, T, Object> wrapper = new ReplicaWaitWriteOperation<>(operation);
				wrapper.setOptions(options.getReplicaWaitOptions());
				operation = wrapper;
			}
			if (options.isMultiExec()) {
				operation = new MultiExecWriteOperation<>(operation);
			}
			operationProcessor = new OperationItemProcessor<>(client, codec, operation);
			operationProcessor.setPoolOptions(options.getPoolOptions());
			operationProcessor.open(executionContext);
		}
	}

	@Override
	public synchronized void close() {
		if (isOpen()) {
			operationProcessor.close();
			operationProcessor = null;
		}
		super.close();
	}

	public boolean isOpen() {
		return operationProcessor != null;
	}

	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		operationProcessor.process(items);
	}

	protected abstract BatchOperation<K, V, T, Object> operation();

}
