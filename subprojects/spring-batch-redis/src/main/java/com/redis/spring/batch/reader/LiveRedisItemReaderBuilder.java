package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.reader.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;

public class LiveRedisItemReaderBuilder<K, V, T extends KeyValue<K, ?>>
		extends RedisItemReaderBuilder<K, V, T, LiveRedisItemReaderBuilder<K, V, T>> {

	public static final int DEFAULT_DATABASE = 0;
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	private static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);

	private Duration flushingInterval = FlushingSimpleStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty();
	private String[] keyPatterns = new String[] { ScanKeyItemReader.DEFAULT_SCAN_MATCH };
	private int database = DEFAULT_DATABASE;
	private int notificationQueueCapacity = LiveKeyItemReader.DEFAULT_QUEUE_CAPACITY;

	public LiveRedisItemReaderBuilder<K, V, T> flushingInterval(Duration flushingInterval) {
		Utils.assertPositive(flushingInterval, "Flushing interval");
		this.flushingInterval = flushingInterval;
		return this;
	}

	public LiveRedisItemReaderBuilder<K, V, T> idleTimeout(Duration idleTimeout) {
		Utils.assertPositive(idleTimeout, "Idle timeout");
		this.idleTimeout = Optional.of(idleTimeout);
		return this;
	}

	public LiveRedisItemReaderBuilder<K, V, T> database(int database) {
		this.database = database;
		return this;
	}

	public LiveRedisItemReaderBuilder<K, V, T> notificationQueueCapacity(int notificationQueueCapacity) {
		this.notificationQueueCapacity = notificationQueueCapacity;
		return this;
	}

	public LiveRedisItemReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
			ValueReaderFactory<K, V, T> valueReaderFactory) {
		super(client, codec, valueReaderFactory);
	}

	public LiveRedisItemReaderBuilder<K, V, T> keyPatterns(String... keyPatterns) {
		this.keyPatterns = keyPatterns;
		return this;
	}

	public LiveRedisItemReader<K, T> build() throws Exception {
		LiveRedisItemReader<K, T> reader = new LiveRedisItemReader<>(jobRunner(), keyReader(), valueReader());
		reader.setFlushingInterval(flushingInterval);
		reader.setIdleTimeout(idleTimeout);
		return configure(reader);
	}

	public LiveKeyItemReader<K> keyReader() {
		LiveKeyItemReader<K> reader = liveKeyItemReader();
		reader.setQueueCapacity(notificationQueueCapacity);
		return reader;
	}

	public static List<String> pubSubPatterns(int database, String... keyPatterns) {
		List<String> patterns = new ArrayList<>();
		for (String keyPattern : keyPatterns) {
			patterns.add(pubSubPattern(database, keyPattern));
		}
		return patterns;
	}

	@SuppressWarnings("unchecked")
	private K[] pubSubPatterns() {
		return (K[]) pubSubPatterns(database, keyPatterns).stream().map(this::encodeKey).toArray();
	}

	public static String pubSubPattern(int database, String keyPattern) {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	private LiveKeyItemReader<K> liveKeyItemReader() {
		if (client instanceof RedisClusterClient) {
			return new LiveRedisClusterKeyItemReader<>(() -> ((RedisClusterClient) client).connectPubSub(codec),
					keyExtractor(), pubSubPatterns());
		}
		return new LiveRedisKeyItemReader<>(() -> ((RedisClient) client).connectPubSub(codec), keyExtractor(),
				pubSubPatterns());
	}

	private Converter<K, K> keyExtractor() {
		return k -> encodeKey(STRING_KEY_EXTRACTOR.convert(decodeKey(k)));
	}

}