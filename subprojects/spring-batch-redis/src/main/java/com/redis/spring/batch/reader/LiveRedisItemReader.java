package com.redis.spring.batch.reader;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader.BaseBuilder;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.ValueType;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.step.FlushingStepOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class LiveRedisItemReader<K, V> extends AbstractRedisItemReader<K, V>
		implements PollableItemReader<KeyValue<K>> {

	public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";

	private KeyspaceNotificationOptions keyspaceNotificationOptions = KeyspaceNotificationOptions.builder().build();
	private FlushingStepOptions flushingOptions = FlushingStepOptions.builder().build();

	public LiveRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, ValueType valueType) {
		super(client, codec, new KeyspaceNotificationItemReader<>(client, codec), valueType);
	}

	@SuppressWarnings("unchecked")
	@Override
	public KeyspaceNotificationItemReader<K, V> getKeyReader() {
		return (KeyspaceNotificationItemReader<K, V>) super.getKeyReader();
	}

	@Override
	protected void doOpen() {
		StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
		try {
			String config = connection.sync().configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
					.getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
			if (!config.contains("K")) {
				throw new ItemStreamException(MessageFormat.format(
						"Keyspace notifications not property configured: {0}={1}. Make sure it contains at least \"K\".",
						CONFIG_NOTIFY_KEYSPACE_EVENTS, config));
			}
		} catch (RedisException e) {
			// CONFIG command might not be available. Ignore.
		}
		getKeyReader().setKeyspaceNotificationOptions(keyspaceNotificationOptions);
		getKeyReader().setScanOptions(options.getScanOptions());
		super.doOpen();
	}

	@Override
	public synchronized KeyValue<K> poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public KeyspaceNotificationOptions getKeyspaceNotificationOptions() {
		return keyspaceNotificationOptions;
	}

	public void setKeyspaceNotificationOptions(KeyspaceNotificationOptions keyspaceNotificationOptions) {
		this.keyspaceNotificationOptions = keyspaceNotificationOptions;
	}

	public FlushingStepOptions getFlushingOptions() {
		return flushingOptions;
	}

	public void setFlushingOptions(FlushingStepOptions options) {
		this.flushingOptions = options;
	}

	@Override
	protected SimpleStepBuilder<K, K> step() {
		return new FlushingStepBuilder<>(super.step()).options(flushingOptions);
	}

	@Override
	protected ItemWriter<KeyValue<K>> queueWriter() {
		ItemWriter<KeyValue<K>> queueWriter = super.queueWriter();
		CompositeItemWriter<KeyValue<K>> compositeWriter = new CompositeItemWriter<>();
		compositeWriter.setDelegates(Arrays.asList(queueWriter, new BigKeyItemWriter()));
		return compositeWriter;
	}

	private class BigKeyItemWriter implements ItemWriter<KeyValue<K>> {

		private final long memLimit = options.getMemoryUsageOptions().getLimit().toBytes();

		@Override
		public void write(List<? extends KeyValue<K>> items) throws Exception {
			List<String> bigKeys = items.stream().filter(v -> v.getMemoryUsage() > memLimit).map(KeyValue::getKey)
					.map(this::toString).collect(Collectors.toList());
			getKeyReader().blockKeys(bigKeys);
		}

		private String toString(K key) {
			return StringCodec.UTF8.decodeKey(codec.encodeKey(key));
		}

	}

	public static Builder<String, String> client(AbstractRedisClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static class Builder<K, V> extends BaseBuilder<K, V, Builder<K, V>> {

		private KeyspaceNotificationOptions keyspaceNotificationOptions = KeyspaceNotificationOptions.builder().build();
		private FlushingStepOptions flushingOptions = FlushingStepOptions.builder().build();

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public Builder<K, V> keyspaceNotificationOptions(KeyspaceNotificationOptions options) {
			this.keyspaceNotificationOptions = options;
			return this;
		}

		public Builder<K, V> flushingOptions(FlushingStepOptions options) {
			this.flushingOptions = options;
			return this;
		}

		public LiveRedisItemReader<K, V> dump() {
			return build(ValueType.DUMP);
		}

		public LiveRedisItemReader<K, V> struct() {
			return build(ValueType.STRUCT);
		}

		public LiveRedisItemReader<K, V> build(ValueType valueType) {
			LiveRedisItemReader<K, V> reader = new LiveRedisItemReader<>(client, codec, valueType);
			configure(reader);
			reader.setFlushingOptions(flushingOptions);
			reader.setKeyspaceNotificationOptions(keyspaceNotificationOptions);
			return reader;
		}

	}

}
