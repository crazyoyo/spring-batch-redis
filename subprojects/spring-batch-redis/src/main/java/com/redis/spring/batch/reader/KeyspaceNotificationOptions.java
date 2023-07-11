package com.redis.spring.batch.reader;

public class KeyspaceNotificationOptions {

	public static final int DEFAULT_DATABASE = 0;
	public static final KeyspaceNotificationOrderingStrategy DEFAULT_ORDERING = KeyspaceNotificationOrderingStrategy.PRIORITY;

	private int database = DEFAULT_DATABASE;
	private KeyspaceNotificationOrderingStrategy orderingStrategy = DEFAULT_ORDERING;
	private QueueOptions queueOptions = QueueOptions.builder().build();

	private KeyspaceNotificationOptions(Builder builder) {
		this.database = builder.database;
		this.orderingStrategy = builder.orderingStrategy;
		this.queueOptions = builder.queueOptions;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
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

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private int database = DEFAULT_DATABASE;
		private KeyspaceNotificationOrderingStrategy orderingStrategy = DEFAULT_ORDERING;
		private QueueOptions queueOptions = QueueOptions.builder().build();

		private Builder() {
		}

		public Builder database(int database) {
			this.database = database;
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

		public KeyspaceNotificationOptions build() {
			return new KeyspaceNotificationOptions(this);
		}
	}

}
