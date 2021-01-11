package org.springframework.batch.item.redis.support;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.item.ChunkOrientedTasklet;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Function;

public class FlushingStepBuilder<I, O> extends SimpleStepBuilder<I, O> {

	private final static Duration DEFAULT_TIMEOUT = Duration.ofMillis(50);

	private Duration timeout = DEFAULT_TIMEOUT;

	public FlushingStepBuilder(StepBuilderHelper<?> parent) {
		super(parent);
	}

	protected FlushingStepBuilder(SimpleStepBuilder<I, O> parent) {
		super(parent);
	}

	@Override
	public FlushingStepBuilder<I, O> chunk(int chunkSize) {
		return (FlushingStepBuilder<I, O>) super.chunk(chunkSize);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Tasklet createTasklet() {
		ItemReader<? extends I> reader = getReader();
		Assert.state(reader != null, "ItemReader must be provided");
		Assert.state(reader instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
		ItemWriter<? super O> writer = getWriter();
		Assert.state(writer != null, "ItemWriter must be provided");
		RepeatOperations repeatOperations = createChunkOperations();
		FlushingChunkProvider<I> chunkProvider = new FlushingChunkProvider<>((PollableItemReader<I>) reader,
				repeatOperations, timeout);
		SimpleChunkProcessor<I, O> chunkProcessor = new SimpleChunkProcessor<>(getProcessor(), writer);
		chunkProvider.setListeners(new ArrayList<>(getItemListeners()));
		chunkProcessor.setListeners(new ArrayList<>(getItemListeners()));
		ChunkOrientedTasklet<I> tasklet = new ChunkOrientedTasklet<>(chunkProvider, chunkProcessor);
		tasklet.setBuffering(!isReaderTransactionalQueue());
		return tasklet;
	}

	public FlushingStepBuilder<I, O> timeout(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	public FlushingStepBuilder<I, O> reader(PollableItemReader<? extends I> reader) {
		return (FlushingStepBuilder<I, O>) super.reader(reader);
	}

	@Override
	public FlushingStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
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

}
