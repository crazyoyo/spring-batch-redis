package com.redis.spring.batch;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.common.queue.ConcurrentSetBlockingQueue;
import com.redis.spring.batch.reader.DedupingByteArrayLinkedBlockingDeque;

class BlockingQueueTests {

	@Test
	void dedupeByteArrays() throws InterruptedException {
		DedupingByteArrayLinkedBlockingDeque queue = new DedupingByteArrayLinkedBlockingDeque(10);
		byte[] array1 = { 1, 2, 3, 4 };
		queue.offer(array1, 1, TimeUnit.SECONDS);
		queue.offer(array1, 1, TimeUnit.SECONDS);
		queue.offer(array1, 1, TimeUnit.SECONDS);
		Assertions.assertEquals(1, queue.size());
		byte[] array2 = { 1, 2, 3, 4 };
		queue.offer(array2, 1, TimeUnit.SECONDS);
		Assertions.assertEquals(1, queue.size());
		byte[] array3 = { 1, 2, 3, 4, 5 };
		queue.offer(array3, 1, TimeUnit.SECONDS);
		Assertions.assertEquals(2, queue.size());
	}

	@Test
	void dedupe() {
		ConcurrentSetBlockingQueue<String> queue = new ConcurrentSetBlockingQueue<>(10);
		queue.offer("12345");
		queue.offer("12345");
		queue.offer("12345");
		Assertions.assertEquals(1, queue.size());
		queue.offer("123456");
		Assertions.assertEquals(2, queue.size());
		queue.offer("1234567");
		Assertions.assertEquals(3, queue.size());
		queue.offer("12345");
		Assertions.assertEquals(3, queue.size());
	}

}
