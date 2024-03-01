package com.redis.spring.batch.reader;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractKeyspaceNotificationListener implements KeyspaceNotificationListener {

	private static final String SEPARATOR = ":";
	private static final Map<String, KeyEvent> eventMap = Stream.of(KeyEvent.values())
			.collect(Collectors.toMap(KeyEvent::getString, Function.identity()));

	private final String pubSubPattern;
	private final BlockingQueue<String> queue;
	private String keyType;

	protected AbstractKeyspaceNotificationListener(String pubSubPattern, BlockingQueue<String> queue) {
		this.pubSubPattern = pubSubPattern;
		this.queue = queue;
	}

	@Override
	public void start() {
		doStart(pubSubPattern);
	}

	@Override
	public void close() {
		doClose(pubSubPattern);
	}

	protected abstract void doClose(String pattern);

	protected abstract void doStart(String pattern);

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	protected boolean notification(String channel, String message) {
		int index = channel.indexOf(SEPARATOR);
		if (index > 0 && acceptType(message)) {
			String key = channel.substring(index + 1);
			return queue.offer(key);
		}
		return false;
	}

	private boolean acceptType(String message) {
		if (keyType == null) {
			return true;
		}
		KeyEvent event = eventMap.getOrDefault(message, KeyEvent.UNKNOWN);
		return keyType.equalsIgnoreCase(event.getType().getString());
	}

}
