package com.redis.spring.batch.reader;

import java.util.concurrent.Callable;

public interface Task extends Callable<Long> {

}