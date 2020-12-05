package org.springframework.batch.item.redis.support;

import java.util.List;

import org.springframework.batch.item.ItemStream;

public interface ValueReader<T> extends ItemStream {

	List<T> read(List<? extends String> keys) throws Exception;

}
