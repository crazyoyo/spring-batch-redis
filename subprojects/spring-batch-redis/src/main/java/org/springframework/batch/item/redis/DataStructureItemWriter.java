package org.springframework.batch.item.redis;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.operation.executor.DataStructureOperationExecutor;
import org.springframework.batch.item.redis.support.operation.executor.OperationExecutor;
import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class DataStructureItemWriter extends OperationItemWriter<String, String, DataStructure> {

	public DataStructureItemWriter(Supplier<StatefulConnection<String, String>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig,
			Function<StatefulConnection<String, String>, RedisModulesAsyncCommands<String, String>> async,
			OperationExecutor<String, String, DataStructure> executor) {
		super(connectionSupplier, poolConfig, async, executor);
	}

	public static DataStructureItemWriterBuilder client(RedisModulesClient client) {
		return new DataStructureItemWriterBuilder(client);
	}

	public static DataStructureItemWriterBuilder client(RedisModulesClusterClient client) {
		return new DataStructureItemWriterBuilder(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class DataStructureItemWriterBuilder
			extends CommandBuilder<String, String, DataStructureItemWriterBuilder> {

		private Converter<StreamMessage<String, String>, XAddArgs> xAddArgs = m -> new XAddArgs().id(m.getId());

		public DataStructureItemWriterBuilder(AbstractRedisClient client) {
			super(client, StringCodec.UTF8);
		}

		public DataStructureItemWriter build() {
			return new DataStructureItemWriter(connectionSupplier(), poolConfig, async(),
					new DataStructureOperationExecutor(client.getDefaultTimeout(), xAddArgs));
		}

	}

}
