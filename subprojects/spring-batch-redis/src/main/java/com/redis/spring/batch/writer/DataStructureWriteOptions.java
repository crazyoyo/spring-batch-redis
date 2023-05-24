package com.redis.spring.batch.writer;

public class DataStructureWriteOptions {

	public enum StreamIdPolicy {
		PROPAGATE, DROP
	}

	public enum MergePolicy {
		MERGE, OVERWRITE
	}

	public static final MergePolicy DEFAULT_MERGE_POLICY = MergePolicy.OVERWRITE;
	public static final StreamIdPolicy DEFAULT_STREAM_ID_POLICY = StreamIdPolicy.PROPAGATE;

	private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;
	private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

	private DataStructureWriteOptions(Builder builder) {
		this.mergePolicy = builder.mergePolicy;
		this.streamIdPolicy = builder.streamIdPolicy;
	}

	public MergePolicy getMergePolicy() {
		return mergePolicy;
	}

	public void setMergePolicy(MergePolicy mergePolicy) {
		this.mergePolicy = mergePolicy;
	}

	public StreamIdPolicy getStreamIdPolicy() {
		return streamIdPolicy;
	}

	public void setStreamIdPolicy(StreamIdPolicy streamIdPolicy) {
		this.streamIdPolicy = streamIdPolicy;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;
		private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

		private Builder() {
		}

		public Builder mergePolicy(MergePolicy mergePolicy) {
			this.mergePolicy = mergePolicy;
			return this;
		}

		public Builder streamIdPolicy(StreamIdPolicy streamIdPolicy) {
			this.streamIdPolicy = streamIdPolicy;
			return this;
		}

		public DataStructureWriteOptions build() {
			return new DataStructureWriteOptions(this);
		}
	}

}
