package com.redis.spring.batch.step;

import java.util.ArrayList;
import java.util.function.Function;

import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.item.ChunkOrientedTasklet;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

public class FlushingStepBuilder<I, O> extends SimpleStepBuilder<I, O> {

	private FlushingStepOptions options = FlushingStepOptions.builder().build();

	public FlushingStepBuilder(StepBuilderHelper<?> parent) {
		super(parent);
	}

	public FlushingStepBuilder(SimpleStepBuilder<I, O> parent) {
		super(parent);
	}

	@Override
	public FlushingFaultTolerantStepBuilder<I, O> faultTolerant() {
		return new FlushingFaultTolerantStepBuilder<>(this);
	}

	@Override
	protected Tasklet createTasklet() {
		ItemReader<? extends I> reader = getReader();
		ItemWriter<? super O> writer = getWriter();
		Assert.state(reader != null, "ItemReader must be provided");
		Assert.state(writer != null, "ItemWriter must be provided");
		FlushingChunkProvider<I> chunkProvider = createChunkProvider();
		SimpleChunkProcessor<I, O> chunkProcessor = createChunkProcessor();
		ChunkOrientedTasklet<I> tasklet = new ChunkOrientedTasklet<>(chunkProvider, chunkProcessor);
		tasklet.setBuffering(!isReaderTransactionalQueue());
		return tasklet;
	}

	private SimpleChunkProcessor<I, O> createChunkProcessor() {
		SimpleChunkProcessor<I, O> chunkProcessor = new SimpleChunkProcessor<>(getProcessor(), getWriter());
		chunkProcessor.setListeners(new ArrayList<>(getItemListeners()));
		return chunkProcessor;
	}

	protected FlushingChunkProvider<I> createChunkProvider() {
		FlushingChunkProvider<I> chunkProvider = new FlushingChunkProvider<>(getReader(), createChunkOperations());
		chunkProvider.setInterval(options.getInterval());
		chunkProvider.setIdleTimeout(options.getIdleTimeout().orElse(null));
		ArrayList<StepListener> listeners = new ArrayList<>(getItemListeners());
		chunkProvider.setListeners(listeners);
		return chunkProvider;
	}

	@Override
	public FlushingStepBuilder<I, O> chunk(int chunkSize) {
		return (FlushingStepBuilder<I, O>) super.chunk(chunkSize);
	}

	@Override
	public FlushingStepBuilder<I, O> chunk(CompletionPolicy completionPolicy) {
		return (FlushingStepBuilder<I, O>) super.chunk(completionPolicy);
	}

	public FlushingStepBuilder<I, O> options(FlushingStepOptions options) {
		this.options = options;
		return this;
	}

	@Override
	public FlushingStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
		Assert.state(reader instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
		return (FlushingStepBuilder<I, O>) super.reader(reader);
	}

	@Override
	public FlushingStepBuilder<I, O> writer(ItemWriter<? super O> writer) {
		return (FlushingStepBuilder<I, O>) super.writer(writer);
	}

	@Override
	public FlushingStepBuilder<I, O> processor(Function<? super I, ? extends O> function) {
		return (FlushingStepBuilder<I, O>) super.processor(function);
	}

	@Override
	public FlushingStepBuilder<I, O> processor(ItemProcessor<? super I, ? extends O> processor) {
		return (FlushingStepBuilder<I, O>) super.processor(processor);
	}

	@Override
	public FlushingStepBuilder<I, O> readerIsTransactionalQueue() {
		return (FlushingStepBuilder<I, O>) super.readerIsTransactionalQueue();
	}

	@Override
	public FlushingStepBuilder<I, O> listener(Object listener) {
		return (FlushingStepBuilder<I, O>) super.listener(listener);
	}

	@Override
	public FlushingStepBuilder<I, O> listener(ItemReadListener<? super I> listener) {
		return (FlushingStepBuilder<I, O>) super.listener(listener);
	}

	@Override
	public FlushingStepBuilder<I, O> listener(ItemWriteListener<? super O> listener) {
		return (FlushingStepBuilder<I, O>) super.listener(listener);
	}

	@Override
	public FlushingStepBuilder<I, O> listener(ItemProcessListener<? super I, ? super O> listener) {
		return (FlushingStepBuilder<I, O>) super.listener(listener);
	}

	@Override
	public FlushingStepBuilder<I, O> chunkOperations(RepeatOperations repeatTemplate) {
		return (FlushingStepBuilder<I, O>) super.chunkOperations(repeatTemplate);
	}

	public FlushingStepOptions getOptions() {
		return options;
	}

}
