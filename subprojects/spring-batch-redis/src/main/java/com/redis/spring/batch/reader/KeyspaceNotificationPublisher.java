package com.redis.spring.batch.reader;

import org.springframework.batch.item.ItemStream;

public interface KeyspaceNotificationPublisher extends ItemStream {

	boolean isOpen();

}
