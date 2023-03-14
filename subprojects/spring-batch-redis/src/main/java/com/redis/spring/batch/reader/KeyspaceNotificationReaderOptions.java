package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KeyspaceNotificationReaderOptions {

	public static final int DEFAULT_DATABASE = 0;
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	protected static final String[] DEFAULT_KEY_PATTERNS = new String[] { ScanOptions.DEFAULT_MATCH };

	private List<String> patterns;
	private QueueOptions queueOptions;

	private KeyspaceNotificationReaderOptions(Builder builder) {
		this.patterns = builder.patterns();
		this.queueOptions = builder.queueOptions;
	}

	public List<String> getPatterns() {
		return patterns;
	}

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		public static final String NOTIFICATION_QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";
		public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
		public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);
		public static final int DEFAULT_DATABASE = 0;

		private int database = DEFAULT_DATABASE;
		private List<String> keyPatterns = Arrays.asList(ScanOptions.DEFAULT_MATCH);
		private QueueOptions queueOptions = QueueOptions.builder().build();

		public Builder database(int database) {
			this.database = database;
			return this;
		}

		public Builder keyPatterns(String... patterns) {
			this.keyPatterns = Arrays.asList(patterns);
			return this;
		}

		public Builder queueOptions(QueueOptions options) {
			this.queueOptions = options;
			return this;
		}

		public KeyspaceNotificationReaderOptions build() {
			return new KeyspaceNotificationReaderOptions(this);
		}

		private List<String> patterns() {
			return keyPatterns.stream().map(p -> String.format(PUBSUB_PATTERN_FORMAT, database, p))
					.collect(Collectors.toList());
		}

	}

}
