package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class ConvertingPollableItemReader<S, T> implements PollableItemReader<T> {

	private final PollableItemReader<S> delegate;
	private final Function<S, T> converter;

	public ConvertingPollableItemReader(PollableItemReader<S> delegate, Function<S, T> converter) {
		this.delegate = delegate;
		this.converter = converter;
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public T read() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
		S item = delegate.read();
		if (item == null) {
			return null;
		}
		return converter.apply(item);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		return converter.apply(delegate.poll(timeout, unit));
	}

}
