package org.springframework.batch.item.redis;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

import lombok.Builder;

@Builder
public class ThrottledWriter<T> implements ItemWriter<T> {

	private final long sleep;

	@Override
	public void write(List<? extends T> items) throws Exception {
		if (sleep > 0) {
			Thread.sleep(sleep);
		}
	}

}
