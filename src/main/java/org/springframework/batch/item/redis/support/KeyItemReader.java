package org.springframework.batch.item.redis.support;

import java.util.Iterator;
import java.util.function.Function;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;

public class KeyItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<K> {

	private final StatefulConnection<K, V> connection;
	private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands;
	private final ScanArgs scanArgs;

	private Iterator<K> keyIterator;
	private KeyScanCursor<K> cursor;

	public KeyItemReader(StatefulConnection<K, V> connection,
			Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands, long scanCount, String scanMatch) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(connection, "A connection is required.");
		Assert.notNull(commands, "A commands supplier is required.");
		Assert.isTrue(scanCount > 0, "Scan count must be greater than 0.");
		Assert.notNull(scanMatch, "Scan match is required.");
		this.connection = connection;
		this.commands = commands;
		this.scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected synchronized void doOpen() {
		if (cursor != null) {
			return;
		}
		cursor = ((RedisKeyCommands<K, ?>) commands.apply(connection)).scan(scanArgs);
		keyIterator = cursor.getKeys().iterator();
	}

	@Override
	protected synchronized void doClose() {
		if (cursor == null) {
			return;
		}
		cursor = null;
		keyIterator = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected synchronized K doRead() throws Exception {
		while (!(keyIterator.hasNext() || cursor.isFinished())) {
			cursor = ((RedisKeyCommands<K, ?>) commands.apply(connection)).scan(cursor, scanArgs);
			keyIterator = cursor.getKeys().iterator();
		}
		if (keyIterator.hasNext()) {
			return keyIterator.next();
		}
		if (cursor.isFinished()) {
			return null;
		}
		throw new IllegalStateException("No more keys but cursor not finished");
	}

}
