package com.redis.spring.batch.reader;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redis.spring.batch.common.DataStructureType;

public class KeyspaceNotificationEnqueuer implements ChannelMessageConsumer {

    private static final String SEPARATOR = ":";

    private static final Map<String, KeyEvent> EVENTS = Stream.of(KeyEvent.values())
            .collect(Collectors.toMap(KeyEvent::getString, Function.identity()));

    private final Log log = LogFactory.getLog(getClass());

    private final BlockingQueue<KeyspaceNotification> queue;

    private DataStructureType type;

    public KeyspaceNotificationEnqueuer(BlockingQueue<KeyspaceNotification> queue) {
        this.queue = queue;
    }

    public void setType(DataStructureType type) {
        this.type = type;
    }

    @Override
    public void message(String channel, String message) {
        String key = channel.substring(channel.indexOf(SEPARATOR) + 1);
        KeyEvent event = keyEvent(message);
        if (type == null || type == event.getType()) {
            KeyspaceNotification notification = new KeyspaceNotification();
            notification.setKey(key);
            notification.setEvent(event);
            if (!enqueue(notification)) {
                log.debug("Keyspace notification queue is full");
            }
        }

    }

    private boolean enqueue(KeyspaceNotification notification) {
        if (queue.remainingCapacity() > 0) {
            return queue.offer(notification);
        }
        return false;
    }

    private static KeyEvent keyEvent(String event) {
        return EVENTS.getOrDefault(event, KeyEvent.UNKNOWN);
    }

}
