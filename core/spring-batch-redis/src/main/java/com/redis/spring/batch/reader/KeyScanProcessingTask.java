package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import io.lettuce.core.ScanIterator;

public class KeyScanProcessingTask<K, V> extends AbstractChunkProcessingTask<K, V> {

	private ScanIterator<K> scanIterator;

	public KeyScanProcessingTask(ScanIterator<K> scanIterator, Function<Iterable<K>, Iterable<V>> function,
			BlockingQueue<V> queue) {
		super(function, queue);
		this.scanIterator = scanIterator;
	}

	@Override
	public void execute() throws InterruptedException {
		while (scanIterator.hasNext()) {
			add(scanIterator.next());
		}
	}

}