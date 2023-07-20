package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SetBlockingQueue<E> implements BlockingQueue<E> {

    private final HashSet<E> set;

    private BlockingQueue<E> delegate;

    public SetBlockingQueue(BlockingQueue<E> delegate) {
        this.set = new HashSet<>();
        this.delegate = delegate;
    }

    @Override
    public E remove() {
        E element = delegate.remove();
        set.remove(element);
        return element;
    }

    @Override
    public E poll() {
        E element = delegate.poll();
        if (element != null) {
            set.remove(element);
        }
        return element;
    }

    @Override
    public E element() {
        return delegate.element();
    }

    @Override
    public E peek() {
        return delegate.peek();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (set.addAll(c)) {
            return delegate.addAll(c);
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        if (set.removeAll(c)) {
            return delegate.removeAll(c);
        }
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        if (set.retainAll(c)) {
            return delegate.retainAll(c);
        }
        return false;
    }

    @Override
    public void clear() {
        set.clear();
        delegate.clear();
    }

    @Override
    public boolean add(E e) {
        if (set.add(e)) {
            return delegate.add(e);
        }
        return true;
    }

    @Override
    public boolean offer(E e) {
        if (set.add(e)) {
            return delegate.offer(e);
        }
        return true;
    }

    @Override
    public void put(E e) throws InterruptedException {
        if (set.add(e)) {
            delegate.put(e);
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (set.add(e)) {
            return delegate.offer(e, timeout, unit);
        }
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        E element = delegate.take();
        set.remove(element);
        return element;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E element = delegate.poll(timeout, unit);
        if (element != null) {
            set.remove(element);
        }
        return element;
    }

    @Override
    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        if (set.remove(o)) {
            return delegate.remove(o);
        }
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        List<E> intermediary = new ArrayList<>();
        int count = delegate.drainTo(intermediary);
        set.removeAll(intermediary);
        c.addAll(intermediary);
        return count;
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        List<E> intermediary = new ArrayList<>();
        int count = delegate.drainTo(intermediary, maxElements);
        set.removeAll(intermediary);
        c.addAll(intermediary);
        return count;
    }

}
