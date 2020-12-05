package org.springframework.batch.item.redis.support;

import java.time.Duration;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class QueueOptions {

	public static final int DEFAULT_CAPACITY = 1000;
	public static final Duration DEFAULT_POLLING_TIMEOUT = Duration.ofMillis(100);

	@Default
	private int capacity = DEFAULT_CAPACITY;
	@Default
	private Duration pollingTimeout = DEFAULT_POLLING_TIMEOUT;
}
