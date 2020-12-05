package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class KeyReaderOptions {

	public static final long DEFAULT_SCAN_COUNT = 1000;
	public static final String DEFAULT_SCAN_MATCH = "*";
	public static final int DEFAULT_SAMPLE_SIZE = 100;

	@Default
	private QueueOptions queueOptions = QueueOptions.builder().build();
	@Default
	private long scanCount = DEFAULT_SCAN_COUNT;
	@Default
	private String scanMatch = DEFAULT_SCAN_MATCH;
	@Default
	private int sampleSize = DEFAULT_SAMPLE_SIZE;

}
