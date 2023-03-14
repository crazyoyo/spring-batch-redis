package com.redis.spring.batch;

import java.util.concurrent.LinkedBlockingDeque;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.common.SetBlockingQueue;

class QueueTests {

	@Test
	void dedupe() {
		SetBlockingQueue<String> queue = new SetBlockingQueue<>(new LinkedBlockingDeque<>(10));
		queue.add("1");
		queue.add("2");
		queue.add("3");
		queue.add("1");
		queue.offer("1");
		queue.offer("2");
		queue.offer("3");
		Assertions.assertEquals(3, queue.size());
	}

}
