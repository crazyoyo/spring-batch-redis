package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractProgressReportingItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
	implements ProgressReporter {

    private Long maxItemCount;

    @Override
    public long getDone() {
	return getCurrentItemCount();
    }

    @Override
    public void setMaxItemCount(int count) {
	this.maxItemCount = (long) count;
	super.setMaxItemCount(count);
    }

    @Override
    public Long getTotal() {
	return maxItemCount;
    }

    @Override
    public void close() throws ItemStreamException {
	log.info("Closing {} - {} items read", ClassUtils.getShortName(getClass()), getCurrentItemCount());
	super.close();
    }
}
