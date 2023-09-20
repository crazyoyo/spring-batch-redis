package com.redis.spring.batch.common;

import java.util.List;

public interface OperationExecutor<I, O> {

    List<O> execute(List<? extends I> items);

}
