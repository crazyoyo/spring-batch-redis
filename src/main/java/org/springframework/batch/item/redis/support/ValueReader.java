package org.springframework.batch.item.redis.support;

import java.util.List;

public interface ValueReader<K, T> {

    List<T> values(List<? extends K> keys) throws Exception;
}
