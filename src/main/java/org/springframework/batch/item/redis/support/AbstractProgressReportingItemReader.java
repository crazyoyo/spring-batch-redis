package org.springframework.batch.item.redis.support;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

@Slf4j
public abstract class AbstractProgressReportingItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements BoundedItemStream {

    private int size;

    protected void setSize(int size) {
        this.size = size;
    }

    @Override
    public void setMaxItemCount(int count) {
        this.size = count;
        super.setMaxItemCount(count);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int available() {
        return size - getCurrentItemCount();
    }
}
