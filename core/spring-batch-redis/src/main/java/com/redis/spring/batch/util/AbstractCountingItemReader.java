package com.redis.spring.batch.util;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

public abstract class AbstractCountingItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> {

    private int maxItemCount = Integer.MAX_VALUE;

    @Override
    public void setMaxItemCount(int count) {
        super.setMaxItemCount(count);
        this.maxItemCount = count;
    }

    public int size() {
        if (maxItemCount == Integer.MAX_VALUE) {
            return -1;
        }
        return maxItemCount - getCurrentItemCount();
    }

}
