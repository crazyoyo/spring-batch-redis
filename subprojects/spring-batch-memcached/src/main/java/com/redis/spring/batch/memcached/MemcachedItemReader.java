package com.redis.spring.batch.memcached;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.memcached.reader.LruMetadumpEntry;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemProcessor;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemReader;

import net.spy.memcached.MemcachedClient;

public class MemcachedItemReader extends AbstractAsyncItemReader<LruMetadumpEntry, MemcachedEntry> {

	private final Supplier<MemcachedClient> clientSupplier;

	public MemcachedItemReader(Supplier<MemcachedClient> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

	@Override
	protected boolean isFlushing() {
		return false;
	}

	@Override
	protected ItemReader<LruMetadumpEntry> reader() {
		return new LruMetadumpItemReader(clientSupplier);
	}

	@Override
	protected ItemProcessor<Iterable<? extends LruMetadumpEntry>, List<MemcachedEntry>> writeProcessor() {
		return new LruMetadumpItemProcessor(clientSupplier);
	}

}
