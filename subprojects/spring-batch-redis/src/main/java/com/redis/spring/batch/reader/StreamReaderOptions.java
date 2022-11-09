package com.redis.spring.batch.reader;

import java.time.Duration;

public class StreamReaderOptions {

	public enum AckPolicy {
		AUTO, MANUAL
	}

	public static final String DEFAULT_OFFSET = StreamItemReader.START_OFFSET;
	public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
	public static final long DEFAULT_COUNT = 50;
	public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.AUTO;

	private String offset = "0-0";
	private Duration block = DEFAULT_BLOCK;
	private long count = DEFAULT_COUNT;
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;

	private StreamReaderOptions(Builder builder) {
		this.offset = builder.offset;
		this.block = builder.block;
		this.count = builder.count;
		this.ackPolicy = builder.ackPolicy;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public Duration getBlock() {
		return block;
	}

	public void setBlock(Duration block) {
		this.block = block;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public AckPolicy getAckPolicy() {
		return ackPolicy;
	}

	public void setAckPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String offset = DEFAULT_OFFSET;
		private Duration block = DEFAULT_BLOCK;
		private long count = DEFAULT_COUNT;
		private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;

		private Builder() {
		}

		public Builder offset(String offset) {
			this.offset = offset;
			return this;
		}

		public Builder block(Duration block) {
			this.block = block;
			return this;
		}

		public Builder count(long count) {
			this.count = count;
			return this;
		}

		public Builder ackPolicy(AckPolicy ackPolicy) {
			this.ackPolicy = ackPolicy;
			return this;
		}

		public StreamReaderOptions build() {
			return new StreamReaderOptions(this);
		}
	}

}
