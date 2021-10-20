package com.redis.spring.batch.support.job;

import java.time.Duration;

import com.redis.spring.batch.support.Timer;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class JobExecutionOptions {

	public static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(3);
	public static final Duration DEFAULT_SLEEP = Duration.ofMillis(1);

	@Default
	private Duration runningTimeout = DEFAULT_RUNNING_TIMEOUT;
	@Default
	private Duration terminationTimeout = DEFAULT_TERMINATION_TIMEOUT;
	@Default
	private Duration sleep = DEFAULT_SLEEP;

	public Timer timer(Duration timeout) {
		return new Timer(timeout, sleep.toMillis());
	}

}