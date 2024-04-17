package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.operation.InitializingOperation;
import com.redis.spring.batch.operation.KeyValueRead;
import com.redis.spring.batch.operation.MappingRedisFuture;
import com.redis.spring.batch.operation.OperationExecutor;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader.StreamMessageIdPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonRead implements InitializingOperation<String, String, String, KeyComparison> {

	private final KeyValueRead<String, String> source;
	private final KeyValueRead<String, String> target;

	private OperationExecutor<String, String, String, KeyValue<String>> targetOperationExecutor;
	private StreamMessageIdPolicy streamMessageIdPolicy;
	private Duration ttlTolerance;
	private AbstractRedisClient targetClient;
	private int targetPoolSize;
	private ReadFrom targetReadFrom;

	public KeyComparisonRead(KeyValueRead<String, String> source, KeyValueRead<String, String> target) {
		this.source = source;
		this.target = target;
	}

	public void setStreamMessageIdPolicy(StreamMessageIdPolicy streamMessageIdPolicy) {
		this.streamMessageIdPolicy = streamMessageIdPolicy;
	}

	public void setTargetClient(AbstractRedisClient targetClient) {
		this.targetClient = targetClient;
	}

	public void setTargetPoolSize(int targetPoolSize) {
		this.targetPoolSize = targetPoolSize;
	}

	public void setTargetReadFrom(ReadFrom targetReadFrom) {
		this.targetReadFrom = targetReadFrom;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	@Override
	public void afterPropertiesSet(StatefulRedisModulesConnection<String, String> connection) throws Exception {
		Assert.notNull(targetClient, "Target Redis client not set");
		Assert.isTrue(targetPoolSize > 0, "Target pool size must be strictly positive");
		Assert.notNull(ttlTolerance, "TTL tolerance not set");
		Assert.notNull(streamMessageIdPolicy, "Stream message ID policy not set");
		source.afterPropertiesSet(connection);
		targetOperationExecutor = new OperationExecutor<>(StringCodec.UTF8, target);
		targetOperationExecutor.setClient(targetClient);
		targetOperationExecutor.setPoolSize(targetPoolSize);
		targetOperationExecutor.setReadFrom(targetReadFrom);
		targetOperationExecutor.afterPropertiesSet();
	}

	@Override
	public void execute(BaseRedisAsyncCommands<String, String> commands, Iterable<? extends String> inputs,
			List<RedisFuture<KeyComparison>> outputs) {
		List<RedisFuture<KeyValue<String>>> sourceOutputs = new ArrayList<>();
		source.execute(commands, inputs, sourceOutputs);
		Map<String, KeyValue<String>> targetItems = targetOperationExecutor.apply(inputs).stream()
				.collect(Collectors.toMap(KeyValue::getKey, Function.identity()));
		Stream<RedisFuture<KeyComparison>> results = sourceOutputs.stream()
				.map(f -> new MappingRedisFuture<>(f, v -> compare(v, targetItems.get(v.getKey()))));
		results.forEach(outputs::add);
	}

	private KeyComparison compare(KeyValue<String> source, KeyValue<String> target) {
		KeyComparison comparison = new KeyComparison();
		comparison.setSource(source);
		comparison.setTarget(target);
		comparison.setStatus(status(source, target));
		return comparison;
	}

	private Status status(KeyValue<String> source, KeyValue<String> target) {
		if (target == null) {
			if (source == null) {
				return Status.OK;
			}
			return Status.MISSING;
		}
		if (!target.exists() && source.exists()) {
			return Status.MISSING;
		}
		if (target.getType() != source.getType()) {
			return Status.TYPE;
		}
		if (!valueEquals(source, target)) {
			return Status.VALUE;
		}
		if (source.getTtl() != target.getTtl()) {
			long delta = Math.abs(source.getTtl() - target.getTtl());
			if (delta > ttlTolerance.toMillis()) {
				return Status.TTL;
			}
		}
		return Status.OK;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private boolean valueEquals(KeyValue<String> source, KeyValue<String> target) {
		if (source.getType() == Type.STREAM) {
			return streamEquals((Collection<StreamMessage>) source.getValue(),
					(Collection<StreamMessage>) target.getValue());
		}
		return Objects.deepEquals(source.getValue(), target.getValue());
	}

	@SuppressWarnings("rawtypes")
	private boolean streamEquals(Collection<StreamMessage> source, Collection<StreamMessage> target) {
		if (CollectionUtils.isEmpty(source)) {
			return CollectionUtils.isEmpty(target);
		}
		if (source.size() != target.size()) {
			return false;
		}
		Iterator<StreamMessage> sourceIterator = source.iterator();
		Iterator<StreamMessage> targetIterator = target.iterator();
		while (sourceIterator.hasNext()) {
			if (!targetIterator.hasNext()) {
				return false;
			}
			StreamMessage sourceMessage = sourceIterator.next();
			StreamMessage targetMessage = targetIterator.next();
			if (!streamMessageEquals(sourceMessage, targetMessage)) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private boolean streamMessageEquals(StreamMessage source, StreamMessage target) {
		if (!Objects.equals(source.getStream(), target.getStream())) {
			return false;
		}
		if (streamMessageIdPolicy == StreamMessageIdPolicy.COMPARE && !Objects.equals(source.getId(), target.getId())) {
			return false;
		}
		Map sourceBody = source.getBody();
		Map targetBody = target.getBody();
		if (CollectionUtils.isEmpty(sourceBody)) {
			return CollectionUtils.isEmpty(targetBody);
		}
		return sourceBody.equals(targetBody);
	}
}