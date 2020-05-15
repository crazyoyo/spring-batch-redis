package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.batch.item.ItemWriter;

import java.util.List;
import java.util.Queue;

@Builder
@AllArgsConstructor
public class QueueItemWriter<T> implements ItemWriter<T> {

    @NonNull
    private final Queue<T> queue;

    @Override
    public void write(List<? extends T> items) {
        queue.addAll(items);
    }
}
