package com.redis.spring.batch.memcached.reader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.memcached.ByteArrayTranscoder;
import com.redis.spring.batch.memcached.MemcachedEntry;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class LruMetadumpItemProcessor
		implements ItemProcessor<Iterable<? extends LruMetadumpEntry>, List<MemcachedEntry>>, ItemStream {

	private final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();
	private final Supplier<MemcachedClient> clientSupplier;

	private MemcachedClient client;

	public LruMetadumpItemProcessor(Supplier<MemcachedClient> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (client == null) {
			client = clientSupplier.get();
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (client != null) {
			client.shutdown();
			client = null;
		}
	}

	@Override
	public List<MemcachedEntry> process(Iterable<? extends LruMetadumpEntry> items) {
		Iterator<String> keys = StreamSupport.stream(items.spliterator(), false).map(LruMetadumpEntry::getKey)
				.iterator();
		Map<String, byte[]> values = client.getBulk(keys, transcoder);
		List<MemcachedEntry> entries = new ArrayList<>();
		for (LruMetadumpEntry metaEntry : items) {
			MemcachedEntry entry = new MemcachedEntry();
			entry.setKey(metaEntry.getKey());
			entry.setValue(values.get(metaEntry.getKey()));
			entry.setExpiration(metaEntry.getExp());
			entries.add(entry);
		}
		return entries;
	}

}
