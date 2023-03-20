package com.redis.spring.batch.reader;

import java.util.Iterator;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final ScanOptions options;
	private StatefulConnection<K, V> connection;
	private KeyScanCursor<K> cursor;
	private Iterator<K> keyIterator;

	public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, ScanOptions options) {
		this.client = client;
		this.codec = codec;
		this.options = options;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (cursor == null) {
			connection = RedisModulesUtils.connection(client, codec);
			RedisKeyCommands<K, V> sync = Utils.sync(connection);
			cursor = sync.scan(args());
			keyIterator = cursor.getKeys().iterator();
		}
	}

	@Override
	public void close() {
		super.close();
		if (cursor != null) {
			cursor = null;
			connection.close();
		}
	}

	private ScanArgs args() {
		KeyScanArgs args = KeyScanArgs.Builder.limit(options.getCount()).match(options.getMatch());
		options.getType().ifPresent(args::type);
		return args;
	}

	@Override
	public synchronized K read() {
		if (keyIterator.hasNext()) {
			return keyIterator.next();
		}
		do {
			if (cursor.isFinished()) {
				return null;
			}
			RedisKeyCommands<K, V> sync = Utils.sync(connection);
			cursor = sync.scan(cursor, args());
			keyIterator = cursor.getKeys().iterator();
		} while (!keyIterator.hasNext());
		return null;
	}

}
