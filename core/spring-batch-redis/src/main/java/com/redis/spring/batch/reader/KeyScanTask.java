package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import io.lettuce.core.ScanIterator;

public class KeyScanTask<K, V> extends AbstractKeyHandler<K, V> {

	private ScanIterator<K> scanIterator;

	public KeyScanTask(ScanIterator<K> scanIterator, Function<Iterable<K>, Iterable<V>> function,
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