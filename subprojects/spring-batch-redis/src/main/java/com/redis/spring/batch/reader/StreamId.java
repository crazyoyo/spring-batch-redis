package com.redis.spring.batch.reader;

public class StreamId implements Comparable<StreamId> {

	public static final StreamId ZERO = StreamId.of(0, 0);

	private final long millis;
	private final long sequence;

	public StreamId(long millis, long sequence) {
		this.millis = millis;
		this.sequence = sequence;
	}

	private static void checkPositive(String id, long number) {
		if (number < 0) {
			throw new IllegalArgumentException(String.format("not an id: %s", id));
		}
	}

	public static StreamId parse(String id) {
		int off = id.indexOf("-");
		if (off == -1) {
			long millis = Long.parseLong(id);
			checkPositive(id, millis);
			return StreamId.of(millis, 0L);
		}
		long millis = Long.parseLong(id.substring(0, off));
		checkPositive(id, millis);
		long sequence = Long.parseLong(id.substring(off + 1));
		checkPositive(id, sequence);
		return StreamId.of(millis, sequence);
	}

	public static StreamId of(long millis, long sequence) {
		return new StreamId(millis, sequence);
	}

	public String toStreamId() {
		return millis + "-" + sequence;
	}

	@Override
	public String toString() {
		return toStreamId();
	}

	@Override
	public int compareTo(StreamId o) {
		long diff = millis - o.millis;
		if (diff != 0) {
			return Long.signum(diff);
		}
		return Long.signum(sequence - o.sequence);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof StreamId)) {
			return false;
		}
		StreamId o = (StreamId) obj;
		return o.millis == millis && o.sequence == sequence;
	}

	@Override
	public int hashCode() {
		long val = millis * 31 * sequence;
		return (int) (val ^ (val >> 32));
	}
}