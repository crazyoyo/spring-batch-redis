package org.springframework.batch.item.redis;

import lombok.Builder;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ThrowingItemWriter<T> extends AbstractItemStreamItemWriter<T> {

        private final Supplier<Exception> exceptionSupplier;
        private final long interval;
        private final AtomicLong index = new AtomicLong();

        @Builder
        public ThrowingItemWriter(Supplier<Exception> exceptionSupplier, long interval) {
            this.exceptionSupplier = exceptionSupplier;
            this.interval = interval;
        }

        @Override
        public void write(List<? extends T> items) throws Exception {
            if (index.getAndIncrement() % interval == 0) {
                throw exceptionSupplier.get();
            }
        }

    }
