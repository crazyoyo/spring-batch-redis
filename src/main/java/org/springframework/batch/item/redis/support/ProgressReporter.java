package org.springframework.batch.item.redis.support;

public interface ProgressReporter {

    /**
     * 
     * @return null if unknown
     */
    Long getTotal();

    long getDone();

}
