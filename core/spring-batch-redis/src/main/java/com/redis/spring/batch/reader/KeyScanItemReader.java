package com.redis.spring.batch.reader;

import org.springframework.batch.item.ExecutionContext;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;

public class KeyScanItemReader<K> implements KeyItemReader<K> {

	private final AbstractRedisClient client;

	private final RedisCodec<K, ?> codec;

	private ReadFrom readFrom;

	private long limit;

	private String match;

	private String type;

	private StatefulRedisModulesConnection<K, ?> connection;

	private ScanIterator<K> iterator;

	public KeyScanItemReader(AbstractRedisClient client, RedisCodec<K, ?> codec) {
		this.client = client;
		this.codec = codec;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public void setLimit(long limit) {
		this.limit = limit;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (iterator == null) {
			connection = ConnectionUtils.connection(client, codec, readFrom);
			iterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
		}
	}

	@Override
	public boolean isOpen() {
		return iterator != null;
	}

	private KeyScanArgs scanArgs() {
		KeyScanArgs args = new KeyScanArgs();
		if (limit > 0) {
			args.limit(limit);
		}
		if (match != null) {
			args.match(match);
		}
		if (type != null) {
			args.type(type);
		}
		return args;
	}

	@Override
	public K read() {
		if (iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

	@Override
	public synchronized void close() {
		if (iterator != null) {
			connection.close();
			iterator = null;
		}
	}

}
