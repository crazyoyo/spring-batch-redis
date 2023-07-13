package com.redis.spring.batch.common;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;

public interface ItemStreamProcessor<I, O> extends ItemProcessor<I, O>, ItemStream {

}
