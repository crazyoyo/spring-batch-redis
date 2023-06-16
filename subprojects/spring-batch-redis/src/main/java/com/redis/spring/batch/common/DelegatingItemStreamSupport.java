package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.ClassUtils;

public class DelegatingItemStreamSupport extends ItemStreamSupport {

	private final List<Object> delegates = new ArrayList<>();

	private boolean open;

	public DelegatingItemStreamSupport(Object... delegates) {
		setName(ClassUtils.getShortName(getClass()));
		this.delegates.addAll(Arrays.asList(delegates));
	}

	protected <T> T addDelegate(T delegate) {
		this.delegates.add(delegate);
		return delegate;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		for (Object delegate : delegates) {
			if (delegate instanceof ItemStreamSupport) {
				((ItemStreamSupport) delegate).setName(name);
			}
		}
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		for (Object delegate : delegates) {
			if (delegate instanceof ItemStream) {
				((ItemStream) delegate).open(executionContext);
			}
		}
		this.open = true;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		for (Object delegate : delegates) {
			if (delegate instanceof ItemStream) {
				((ItemStream) delegate).update(executionContext);
			}
		}
	}

	@Override
	public void close() {
		for (Object delegate : delegates) {
			if (delegate instanceof ItemStream) {
				((ItemStream) delegate).close();
			}
		}
		super.close();
		this.open = false;
	}

	public boolean isOpen() {
		return open;
	}

}
