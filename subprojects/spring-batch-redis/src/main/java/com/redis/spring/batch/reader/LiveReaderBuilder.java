package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class LiveReaderBuilder<K, V, T extends KeyValue<K>> {

	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private final JobRunner jobRunner;
	private final ItemProcessor<List<K>, List<T>> valueReader;
	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private StepOptions stepOptions = StepOptions.builder().flushingInterval(DEFAULT_FLUSHING_INTERVAL).build();
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private KeyspaceNotificationReaderOptions notificationReaderOptions = KeyspaceNotificationReaderOptions.builder()
			.build();
	private Optional<Predicate<K>> keyFilter = Optional.empty();

	public LiveReaderBuilder(JobRunner jobRunner, ItemProcessor<List<K>, List<T>> valueReader,
			AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.jobRunner = jobRunner;
		this.valueReader = valueReader;
		this.client = client;
		this.codec = codec;
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

	public LiveReaderBuilder<K, V, T> notificationReaderOptions(KeyspaceNotificationReaderOptions options) {
		this.notificationReaderOptions = options;
		return this;
	}

	private ItemProcessor<K, K> keyProcessor() {
		if (keyFilter.isPresent()) {
			return new FilteringItemProcessor<>(keyFilter.get());
		}
		return null;
	}

	public LiveRedisItemReader<K, T> build() {
		return new LiveRedisItemReader<>(jobRunner, keyReader(), keyProcessor(), valueReader, stepOptions,
				queueOptions);
	}

	@SuppressWarnings("unchecked")
	public PollableItemReader<K> keyReader() {
		KeyspaceNotificationItemReader keyReader = new KeyspaceNotificationItemReader(client);
		keyReader.setOptions(notificationReaderOptions);
		if (codec == StringCodec.UTF8) {
			return (PollableItemReader<K>) keyReader;
		}
		return new ConvertingPollableItemReader<>(keyReader, new KeyspaceNotificationConverter<>(codec));
	}

	private static class KeyspaceNotificationConverter<K> implements Converter<String, K> {

		private final RedisCodec<K, ?> codec;

		public KeyspaceNotificationConverter(RedisCodec<K, ?> codec) {
			this.codec = codec;
		}

		@Override
		public K convert(String source) {
			return codec.decodeKey(StringCodec.UTF8.encodeKey(source));
		}

	}

}