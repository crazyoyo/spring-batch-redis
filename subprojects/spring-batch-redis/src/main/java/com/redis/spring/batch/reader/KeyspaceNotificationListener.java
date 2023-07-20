package com.redis.spring.batch.reader;

public interface KeyspaceNotificationListener {

    void queueFull(KeyspaceNotification notitication);

}
