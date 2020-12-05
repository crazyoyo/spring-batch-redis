package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class LiveKeyReaderOptions {

	public static final int DEFAULT_DATABASE = 0;
	public static final String DEFAULT_KEY_PATTERN = "*";

	@Default
	private QueueOptions queueOptions = QueueOptions.builder().build();
	@Default
	private String keyPattern = DEFAULT_KEY_PATTERN;
	@Default
	private int database = DEFAULT_DATABASE;

}
