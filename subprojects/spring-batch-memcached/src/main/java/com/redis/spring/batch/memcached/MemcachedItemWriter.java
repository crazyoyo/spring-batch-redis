package com.redis.spring.batch.memcached;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.memcached.common.ByteArrayTranscoder;
import com.redis.spring.batch.memcached.common.MemcachedException;
import com.redis.spring.batch.memcached.common.MemcachedUtils;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;

public class MemcachedItemWriter implements ItemStreamWriter<MemcachedEntry> {

	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

	private static final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

	private final Supplier<MemcachedClient> clientSupplier;
	private Duration timeout = DEFAULT_TIMEOUT;

	private MemcachedClient client;

	public MemcachedItemWriter(Supplier<MemcachedClient> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (client == null) {
			client = clientSupplier.get();
		}
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (client != null) {
			client.shutdown();
			client = null;
		}
	}

	@Override
	public void write(Chunk<? extends MemcachedEntry> chunk) throws Exception {
		Stream<OperationFuture<Boolean>> stream = chunk.getItems().stream().map(this::write);
		List<Boolean> results = MemcachedUtils.getAll(timeout, stream.collect(Collectors.toList()));
		for (int index = 0; index < results.size(); index++) {
			Boolean result = results.get(index);
			if (!Boolean.TRUE.equals(result)) {
				throw new MemcachedException("Could not write key " + chunk.getItems().get(index).getKey());
			}
		}
	}

	private OperationFuture<Boolean> write(MemcachedEntry entry) {
		return client.set(entry.getKey(), entry.getExpiration(), entry.getValue(), transcoder);
	}

	public Duration getTimeout() {
		return timeout;
	}

	public void setTimeout(Duration timeout) {
		this.timeout = timeout;
	}
}
