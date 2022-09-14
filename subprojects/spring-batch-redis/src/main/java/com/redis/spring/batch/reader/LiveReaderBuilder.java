package com.redis.spring.batch.reader;

import java.util.List;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class LiveReaderBuilder<K, V, T extends KeyValue<K>> {

	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);

	private final JobRunner jobRunner;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	private final StatefulRedisPubSubConnection<K, V> pubSubConnection;
	private final Converter<K, K> keyExtractor;
	private LiveReaderOptions options = LiveReaderOptions.builder().build();
	private final K[] pubSubPatterns;

	public LiveReaderBuilder(JobRunner jobRunner, ItemProcessor<List<? extends K>, List<T>> valueReader,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, K[] pubSubPatterns,
			Converter<K, K> eventKeyExtractor) {
		this.jobRunner = jobRunner;
		this.valueReader = valueReader;
		this.pubSubConnection = pubSubConnection;
		this.pubSubPatterns = pubSubPatterns;
		this.keyExtractor = eventKeyExtractor;
	}

	public LiveReaderBuilder<K, V, T> options(LiveReaderOptions options) {
		this.options = options;
		return this;
	}

	public LiveRedisItemReader<K, T> build() {
		return new LiveRedisItemReader<>(keyReader(), valueReader, jobRunner, options);
	}

	public KeyspaceNotificationItemReader<K> keyReader() {
		return new KeyspaceNotificationItemReader<>(keyspaceNotificationPublisher(), keyExtractor, pubSubPatterns,
				options.getNotificationQueueOptions());
	}

	private KeyspaceNotificationPublisher<K> keyspaceNotificationPublisher() {
		if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
			return new RedisClusterKeyspaceNotificationPublisher<>(
					(StatefulRedisClusterPubSubConnection<K, V>) pubSubConnection);
		}
		return new RedisKeyspaceNotificationPublisher<>(pubSubConnection);
	}

	private static <K> K encodeKey(RedisCodec<K, ?> codec, String key) {
		return codec.decodeKey(StringCodec.UTF8.encodeKey(key));
	}

	private static <K> String decodeKey(RedisCodec<K, ?> codec, K key) {
		return StringCodec.UTF8.decodeKey(codec.encodeKey(key));
	}

	public static <K> Converter<K, K> keyExtractor(RedisCodec<K, ?> codec) {
		return k -> encodeKey(codec, STRING_KEY_EXTRACTOR.convert(decodeKey(codec, k)));
	}

	@SuppressWarnings("unchecked")
	public static <K> K[] pubSubPatterns(RedisCodec<K, ?> codec, int database, String... keyPatterns) {
		return (K[]) Stream.of(pubSubPatterns(database, keyPatterns)).map(s -> encodeKey(codec, s)).toArray();
	}

	public static String[] pubSubPatterns(int database, String... keyPatterns) {
		if (ObjectUtils.isEmpty(keyPatterns)) {
			return new String[] { pubSubPattern(database, ScanReaderOptions.DEFAULT_MATCH) };
		}
		String[] patterns = new String[keyPatterns.length];
		for (int index = 0; index < keyPatterns.length; index++) {
			patterns[index] = pubSubPattern(database, keyPatterns[index]);
		}
		return patterns;
	}

	public static String pubSubPattern(int database, String keyPattern) {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

}