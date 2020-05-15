package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

@Builder
@AllArgsConstructor
public class ProcessingItemWriter<S, T> implements ItemWriter<S> {

    @NonNull
    private final ItemProcessor<List<? extends S>, List<? extends T>> processor;
    @NonNull
    private final ItemWriter<T> writer;

    @Override
    public void write(List<? extends S> items) throws Exception {
        writer.write(processor.process(items));
    }
}
