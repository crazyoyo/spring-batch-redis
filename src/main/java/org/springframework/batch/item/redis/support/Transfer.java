package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class Transfer<I, O> {

	private final String name;
	private final ItemReader<I> reader;
	private final ItemProcessor<I, O> processor;
	private final ItemWriter<O> writer;
	@Default
	private TransferOptions options = TransferOptions.builder().build();

}
