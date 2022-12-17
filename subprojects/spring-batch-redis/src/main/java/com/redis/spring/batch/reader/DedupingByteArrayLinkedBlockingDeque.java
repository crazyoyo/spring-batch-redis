package com.redis.spring.batch.reader;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class DedupingByteArrayLinkedBlockingDeque extends LinkedBlockingDeque<byte[]> {

	private static final long serialVersionUID = 1L;

	public DedupingByteArrayLinkedBlockingDeque() {
		super();
	}

	public DedupingByteArrayLinkedBlockingDeque(Collection<? extends byte[]> c) {
		super(c);
	}

	public DedupingByteArrayLinkedBlockingDeque(int capacity) {
		super(capacity);
	}

	@Override
	public boolean offer(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.offer(e);
	}

	@Override
	public boolean offer(byte[] e, long timeout, TimeUnit unit) throws InterruptedException {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.offer(e, timeout, unit);
	}

	@Override
	public boolean add(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.add(e);
	}

	@Override
	public boolean addAll(Collection<? extends byte[]> c) {
		c.forEach(e -> removeIf(e2 -> Arrays.equals(e2, e)));
		return super.addAll(c);
	}

	@Override
	public void addFirst(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		super.addFirst(e);
	}

	@Override
	public void addLast(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		super.addLast(e);
	}

	@Override
	public boolean offerFirst(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.offerFirst(e);
	}

	@Override
	public boolean offerLast(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.offerLast(e);
	}

	@Override
	public boolean offerFirst(byte[] e, long timeout, TimeUnit unit) throws InterruptedException {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.offerFirst(e, timeout, unit);
	}

	@Override
	public boolean offerLast(byte[] e, long timeout, TimeUnit unit) throws InterruptedException {
		removeIf(e2 -> Arrays.equals(e2, e));
		return super.offerLast(e, timeout, unit);
	}

	@Override
	public void push(byte[] e) {
		removeIf(e2 -> Arrays.equals(e2, e));
		super.push(e);
	}

	@Override
	public void put(byte[] e) throws InterruptedException {
		removeIf(e2 -> Arrays.equals(e2, e));
		super.put(e);
	}

	@Override
	public void putFirst(byte[] e) throws InterruptedException {
		removeIf(e2 -> Arrays.equals(e2, e));
		super.putFirst(e);
	}

	@Override
	public void putLast(byte[] e) throws InterruptedException {
		removeIf(e2 -> Arrays.equals(e2, e));
		super.putLast(e);
	}

}
