package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.operation.RestoreReplace;
import org.springframework.batch.item.redis.support.operation.executor.OperationExecutor;
import org.springframework.batch.item.redis.support.operation.executor.SimpleOperationExecutor;

import java.util.function.Function;
import java.util.function.Supplier;

public class KeyDumpItemWriter extends OperationItemWriter<String, String, KeyValue<byte[]>> {

	public KeyDumpItemWriter(Supplier<StatefulConnection<String, String>> statefulConnectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig,
			Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async,
			OperationExecutor<String, String, KeyValue<byte[]>> executor) {
		super(statefulConnectionSupplier, poolConfig, async, executor);
	}

	public static KeyDumpItemWriterBuilder client(RedisModulesClient client) {
		return new KeyDumpItemWriterBuilder(client);
	}

	public static KeyDumpItemWriterBuilder client(RedisModulesClusterClient client) {
		return new KeyDumpItemWriterBuilder(client);
	}

	public static class KeyDumpItemWriterBuilder extends CommandBuilder<String, String, KeyDumpItemWriterBuilder> {

		public KeyDumpItemWriterBuilder(AbstractRedisClient client) {
			super(client, StringCodec.UTF8);
		}

		public KeyDumpItemWriter build() {
			RestoreReplace<String, String, KeyValue<byte[]>> operation = new RestoreReplace<>(KeyValue::getKey,
					KeyValue<byte[]>::getValue, KeyValue::getAbsoluteTTL);
			return new KeyDumpItemWriter(connectionSupplier(), poolConfig, async(),
					new SimpleOperationExecutor<>(operation));
		}

	}

}
