package com.redis.spring.batch.memcached;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.memcached.common.ByteArrayTranscoder;
import com.redis.spring.batch.memcached.reader.LruMetadumpEntry;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemReader;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class MemcachedItemReader extends AbstractAsyncItemReader<LruMetadumpEntry, MemcachedEntry> {

	private static final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

	private final Supplier<MemcachedClient> clientSupplier;
	private MemcachedClient processorClient;

	public MemcachedItemReader(Supplier<MemcachedClient> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		processorClient = clientSupplier.get();
		super.doOpen();
	}

	@Override
	protected synchronized void doClose() throws TimeoutException, InterruptedException {
		if (processorClient != null) {
			processorClient.shutdown();
			processorClient = null;
		}
		super.doClose();
	}

	@Override
	protected ItemReader<LruMetadumpEntry> reader() {
		return new LruMetadumpItemReader(clientSupplier);
	}

	@Override
	protected List<MemcachedEntry> read(Iterable<? extends LruMetadumpEntry> chunk) {
		Iterator<String> keys = StreamSupport.stream(chunk.spliterator(), false).map(LruMetadumpEntry::getKey)
				.iterator();
		Map<String, byte[]> values = processorClient.getBulk(keys, transcoder);
		List<MemcachedEntry> entries = new ArrayList<>();
		for (LruMetadumpEntry metaEntry : chunk) {
			MemcachedEntry entry = new MemcachedEntry();
			entry.setKey(metaEntry.getKey());
			entry.setValue(values.get(metaEntry.getKey()));
			entry.setExpiration(metaEntry.getExp());
			entries.add(entry);
		}
		return entries;
	}

}
