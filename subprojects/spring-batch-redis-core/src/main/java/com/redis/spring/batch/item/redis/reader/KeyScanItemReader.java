package com.redis.spring.batch.item.redis.reader;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.common.KeyEvent;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;

public class KeyScanItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<KeyEvent<K>> {

	public static final String EVENT = "scan";

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private ReadFrom readFrom;
	private KeyScanArgs scanArgs = new KeyScanArgs();

	private StatefulRedisModulesConnection<K, V> connection;
	private ScanIterator<K> iterator;

	public KeyScanItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (connection == null) {
			connection = RedisModulesUtils.connection(client, codec, readFrom);
		}
		if (iterator == null) {
			iterator = ScanIterator.scan(connection.sync(), scanArgs);
		}
	}

	@Override
	protected synchronized void doClose() throws Exception {
		iterator = null;
		if (connection != null) {
			connection.close();
			connection = null;
		}
	}

	@Override
	protected KeyEvent<K> doRead() {
		if (iterator.hasNext()) {
			return keyEvent(iterator.next());
		}
		return null;
	}

	private KeyEvent<K> keyEvent(K key) {
		return KeyEvent.of(key, EVENT);
	}

	public KeyScanArgs getScanArgs() {
		return scanArgs;
	}

	public void setScanArgs(KeyScanArgs args) {
		this.scanArgs = args;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

}
