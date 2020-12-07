package org.springframework.batch.item.redis;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class DataGeneratorOptions {

	private static final int DEFAULT_START = 0;
	private static final int DEFAULT_END = 1000;

	@Default
	private int start = DEFAULT_START;
	@Default
	private int end = DEFAULT_END;
	private long sleep;

}
