package com.redis.spring.batch.reader;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.spring.batch.common.FlushingStepOptions;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;

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
	private FlushingStepOptions stepOptions = StepOptions.builder().flushing().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private QueueOptions notificationQueueOptions = QueueOptions.builder().build();

	private final K[] pubSubPatterns;
	private Optional<Predicate<K>> keyFilter = Optional.empty();

	public LiveReaderBuilder(JobRunner jobRunner, ItemProcessor<List<? extends K>, List<T>> valueReader,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, K[] pubSubPatterns, Converter<K, K> keyExtractor) {
		this.jobRunner = jobRunner;
		this.valueReader = valueReader;
		this.pubSubConnection = pubSubConnection;
		this.pubSubPatterns = pubSubPatterns;
		this.keyExtractor = keyExtractor;
	}

	public LiveReaderBuilder<K, V, T> keyFilter(Predicate<K> keyFilter) {
		this.keyFilter = Optional.of(keyFilter);
		return this;
	}

	public LiveReaderBuilder<K, V, T> stepOptions(FlushingStepOptions options) {
		this.stepOptions = options;
		return this;
	}

	public LiveReaderBuilder<K, V, T> queueOptions(QueueOptions options) {
		this.queueOptions = options;
		return this;
	}

	public LiveReaderBuilder<K, V, T> notificationQueueOptions(QueueOptions options) {
		this.notificationQueueOptions = options;
		return this;
	}

	public LiveRedisItemReader<K, T> build() {
		KeyspaceNotificationItemReader<K> keyReader = new KeyspaceNotificationItemReader<>(notificationPublisher(),
				keyExtractor, pubSubPatterns, notificationQueueOptions);
		keyFilter.ifPresent(keyReader::setFilter);
		return new LiveRedisItemReader<>(jobRunner, keyReader, valueReader, stepOptions, queueOptions);
	}

	private KeyspaceNotificationPublisher<K> notificationPublisher() {
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
			return new String[] { pubSubPattern(database, ScanOptions.DEFAULT_MATCH) };
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