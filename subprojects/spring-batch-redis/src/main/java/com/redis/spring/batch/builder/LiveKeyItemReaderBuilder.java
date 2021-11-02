package com.redis.spring.batch.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.support.LiveKeyItemReader;
import com.redis.spring.batch.support.LiveRedisClusterKeyItemReader;
import com.redis.spring.batch.support.LiveRedisKeyItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class LiveKeyItemReaderBuilder {

	private static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);

	public static final int DEFAULT_DATABASE = 0;
	public static final String DEFAULT_KEY_PATTERN = "*";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

	private List<String> keyPatterns = Arrays.asList(DEFAULT_KEY_PATTERN);
	private int database = DEFAULT_DATABASE;
	private int queueCapacity = LiveKeyItemReader.DEFAULT_QUEUE_CAPACITY;

	public static List<String> pubSubPatterns(int database, String... keyPatterns) {
		List<String> patterns = new ArrayList<>();
		for (String keyPattern : keyPatterns) {
			patterns.add(pubSubPattern(database, keyPattern));
		}
		return patterns;
	}

	private List<String> pubSubPatterns() {
		return pubSubPatterns(database, keyPatterns.toArray(new String[0]));
	}

	public static String pubSubPattern(int database, String keyPattern) {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	private final AbstractRedisClient client;

	public LiveKeyItemReaderBuilder(AbstractRedisClient client) {
		this.client = client;
	}

	public LiveKeyItemReader<String> build() {
		LiveKeyItemReader<String> reader = reader();
		reader.setQueueCapacity(queueCapacity);
		return reader;
	}

	private LiveKeyItemReader<String> reader() {
		if (client instanceof RedisClusterClient) {
			return new LiveRedisClusterKeyItemReader<>(((RedisClusterClient) client)::connectPubSub,
					STRING_KEY_EXTRACTOR, pubSubPatterns());
		}
		return new LiveRedisKeyItemReader<>(((RedisClient) client)::connectPubSub, STRING_KEY_EXTRACTOR,
				pubSubPatterns());
	}

}