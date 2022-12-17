package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.common.queue.ConcurrentSetBlockingQueue;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class LiveReaderBuilder<K, V, T extends KeyValue<K>> {

	public static final String NOTIFICATION_QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);
	public static final int DEFAULT_DATABASE = 0;

	private final JobRunner jobRunner;
	private final ItemProcessor<List<K>, List<T>> valueReader;
	private final StatefulRedisPubSubConnection<K, V> connection;
	private final RedisCodec<K, V> codec;

	private int database = DEFAULT_DATABASE;
	private List<String> keyPatterns = Arrays.asList(ScanOptions.DEFAULT_MATCH);
	private StepOptions stepOptions = StepOptions.builder().flushingInterval(DEFAULT_FLUSHING_INTERVAL).build();
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private QueueOptions notificationQueueOptions = QueueOptions.builder().build();
	private Optional<Predicate<K>> keyFilter = Optional.empty();

	public LiveReaderBuilder(JobRunner jobRunner, ItemProcessor<List<K>, List<T>> valueReader,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, RedisCodec<K, V> codec) {
		this.jobRunner = jobRunner;
		this.valueReader = valueReader;
		this.connection = pubSubConnection;
		this.codec = codec;
	}

	public LiveReaderBuilder<K, V, T> database(int database) {
		this.database = database;
		return this;
	}

	public LiveReaderBuilder<K, V, T> keyPatterns(String... patterns) {
		this.keyPatterns = Arrays.asList(patterns);
		return this;
	}

	public LiveReaderBuilder<K, V, T> keyFilter(Predicate<K> filter) {
		this.keyFilter = Optional.of(filter);
		return this;
	}

	public LiveReaderBuilder<K, V, T> stepOptions(StepOptions options) {
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
		return new LiveRedisItemReader<>(jobRunner, keyReader(), keyProcessor(), valueReader, stepOptions,
				queueOptions);
	}

	private ItemProcessor<K, K> keyProcessor() {
		if (keyFilter.isPresent()) {
			return new FilteringItemProcessor<>(keyFilter.get());
		}
		return null;
	}

	public PollableItemReader<K> keyReader() {
		BlockingQueue<K> queue = notificationQueue();
		Utils.createGaugeCollectionSize(NOTIFICATION_QUEUE_SIZE_GAUGE_NAME, queue);
		if (connection instanceof StatefulRedisClusterPubSubConnection) {
			return new RedisClusterKeyspaceNotificationItemReader<>(
					(StatefulRedisClusterPubSubConnection<K, V>) connection, pubSubPatterns(), keyExtractor(), queue);
		}
		return new RedisKeyspaceNotificationItemReader<>(connection, pubSubPatterns(), keyExtractor(), queue);
	}

	@SuppressWarnings("unchecked")
	private BlockingQueue<K> notificationQueue() {
		if (codec == ByteArrayCodec.INSTANCE) {
			return (BlockingQueue<K>) new DedupingByteArrayLinkedBlockingDeque(notificationQueueOptions.getCapacity());
		}
		return new ConcurrentSetBlockingQueue<>(notificationQueueOptions.getCapacity());
	}

	private static <K> K encodeKey(RedisCodec<K, ?> codec, String key) {
		return codec.decodeKey(StringCodec.UTF8.encodeKey(key));
	}

	private static <K> String decodeKey(RedisCodec<K, ?> codec, K key) {
		return StringCodec.UTF8.decodeKey(codec.encodeKey(key));
	}

	@SuppressWarnings("unchecked")
	private Converter<K, K> keyExtractor() {
		if (codec == StringCodec.UTF8) {
			return (Converter<K, K>) STRING_KEY_EXTRACTOR;
		}
		return k -> encodeKey(codec, STRING_KEY_EXTRACTOR.convert(decodeKey(codec, k)));
	}

	@SuppressWarnings("unchecked")
	private K[] pubSubPatterns() {
		Stream<String> patterns = keyPatterns.stream().map(p -> String.format(PUBSUB_PATTERN_FORMAT, database, p));
		if (codec == StringCodec.UTF8) {
			return (K[]) patterns.toArray();
		}
		return (K[]) patterns.map(s -> encodeKey(codec, s)).toArray();
	}

}