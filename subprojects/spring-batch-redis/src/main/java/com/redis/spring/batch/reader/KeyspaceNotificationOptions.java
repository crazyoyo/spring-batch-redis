package com.redis.spring.batch.reader;

import java.util.Optional;

public class KeyspaceNotificationOptions {

	public static final int DEFAULT_DATABASE = 0;
	public static final KeyspaceNotificationOrderingStrategy DEFAULT_ORDERING = KeyspaceNotificationOrderingStrategy.PRIORITY;

	private int database = DEFAULT_DATABASE;
	private String match = ScanOptions.DEFAULT_MATCH;
	private Optional<String> type = Optional.empty();
	private KeyspaceNotificationOrderingStrategy orderingStrategy = DEFAULT_ORDERING;
	private QueueOptions queueOptions = QueueOptions.builder().build();

	private KeyspaceNotificationOptions(Builder builder) {
		this.database = builder.database;
		this.match = builder.match;
		this.orderingStrategy = builder.orderingStrategy;
		this.queueOptions = builder.queueOptions;
		this.type = builder.type;
	}

	/**
	 * Set key type to retain. Empty means include all types.
	 * 
	 */
	public void setType(Optional<String> type) {
		this.type = type;
	}

	public void setType(String type) {
		setType(Optional.of(type));
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public String getMatch() {
		return match;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public KeyspaceNotificationOrderingStrategy getOrderingStrategy() {
		return orderingStrategy;
	}

	public void setOrderingStrategy(KeyspaceNotificationOrderingStrategy orderingStrategy) {
		this.orderingStrategy = orderingStrategy;
	}

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
	}

	public Optional<String> getType() {
		return type;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private int database = DEFAULT_DATABASE;
		private String match = ScanOptions.DEFAULT_MATCH;
		private KeyspaceNotificationOrderingStrategy orderingStrategy = DEFAULT_ORDERING;
		private QueueOptions queueOptions = QueueOptions.builder().build();
		private Optional<String> type = Optional.empty();

		private Builder() {
		}

		public Builder database(int database) {
			this.database = database;
			return this;
		}

		public Builder match(String match) {
			this.match = match;
			return this;
		}

		public Builder orderingStrategy(KeyspaceNotificationOrderingStrategy orderingStrategy) {
			this.orderingStrategy = orderingStrategy;
			return this;
		}

		public Builder queueOptions(QueueOptions queueOptions) {
			this.queueOptions = queueOptions;
			return this;
		}

		public Builder type(Optional<String> type) {
			this.type = type;
			return this;
		}

		public KeyspaceNotificationOptions build() {
			return new KeyspaceNotificationOptions(this);
		}
	}

}
