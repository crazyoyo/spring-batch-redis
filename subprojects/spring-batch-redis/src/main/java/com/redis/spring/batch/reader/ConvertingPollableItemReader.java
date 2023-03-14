package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.core.convert.converter.Converter;

public class ConvertingPollableItemReader<S, T> implements PollableItemReader<T> {

	private final PollableItemReader<S> delegate;
	private final Converter<S, T> converter;

	public ConvertingPollableItemReader(PollableItemReader<S> delegate, Converter<S, T> converter) {
		this.delegate = delegate;
		this.converter = converter;
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		S item = delegate.read();
		if (item == null) {
			return null;
		}
		return converter.convert(item);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		return converter.convert(delegate.poll(timeout, unit));
	}

}
