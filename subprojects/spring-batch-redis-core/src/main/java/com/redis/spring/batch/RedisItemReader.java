package com.redis.spring.batch;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.operation.Evalsha;
import com.redis.spring.batch.operation.MappingOperation;
import com.redis.spring.batch.operation.Operation;
import com.redis.spring.batch.operation.OperationExecutor;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.EvalFunction;
import com.redis.spring.batch.reader.EvalStructFunction;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisItemReader<K, V> extends AbstractRedisItemReader<K, V, KeyValue<K>> {

	public enum ValueType {
		DUMP, STRUCT, TYPE
	}

	public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	public static final ValueType DEFAULT_TYPE = ValueType.DUMP;
	public static final int MEM_LIMIT_NONE = 0;
	public static final int MEM_LIMIT_FETCH = -1;
	public static final DataSize DEFAULT_MEM_USAGE_LIMIT = DataSize.ofBytes(0);// No mem limit by default
	public static final int DEFAULT_MEM_USAGE_SAMPLES = 5;

	private static final String SCRIPT_FILENAME = "keyvalue.lua";

	private final ValueType type;

	private int poolSize = DEFAULT_POOL_SIZE;
	private DataSize memUsageLimit = DEFAULT_MEM_USAGE_LIMIT;
	private int memUsageSamples = DEFAULT_MEM_USAGE_SAMPLES;
	private OperationExecutor<K, V, K, KeyValue<K>> operationExecutor;

	public RedisItemReader(RedisCodec<K, V> codec, ValueType type) {
		super(codec);
		this.type = type;
	}

	protected Operation<K, V, K, KeyValue<K>> operation() {
		String lua;
		try {
			lua = BatchUtils.readFile(SCRIPT_FILENAME);
		} catch (IOException e) {
			throw new ItemStreamException("Could not read LUA script file " + SCRIPT_FILENAME, e);
		}
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			String digest = connection.sync().scriptLoad(lua);
			Evalsha<K, V, K> operation = new Evalsha<>(digest, codec, Function.identity());
			operation.setArgs(type.name().toLowerCase(), memUsageLimit.toBytes(), memUsageSamples);
			return new MappingOperation<>(operation, evalFunction());
		}
	}

	@Override
	protected synchronized void doOpen() {
		if (operationExecutor == null) {
			operationExecutor = operationExecutor();
		}
		super.doOpen();
	}

	@Override
	protected synchronized void doClose() throws InterruptedException {
		super.doClose();
		if (operationExecutor != null) {
			operationExecutor.close();
			operationExecutor = null;
		}
	}

	@Override
	protected List<KeyValue<K>> read(Iterable<? extends K> keys) {
		return operationExecutor.apply(keys);
	}

	public OperationExecutor<K, V, K, KeyValue<K>> operationExecutor() {
		Assert.notNull(client, "Redis client not set");
		return new OperationExecutor<>(pool(), operation());
	}

	private GenericObjectPool<StatefulRedisModulesConnection<K, V>> pool() {
		GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(poolSize);
		return ConnectionPoolSupport.createGenericObjectPool(this::connection, config);
	}

	private Function<List<Object>, KeyValue<K>> evalFunction() {
		if (type == ValueType.STRUCT) {
			return new EvalStructFunction<>(codec);
		}
		return new EvalFunction<>(codec);
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void setMemUsageLimit(DataSize limit) {
		this.memUsageLimit = limit;
	}

	public void setMemUsageSamples(int samples) {
		this.memUsageSamples = samples;
	}

	public static RedisItemReader<byte[], byte[]> dump() {
		return new RedisItemReader<>(ByteArrayCodec.INSTANCE, ValueType.DUMP);
	}

	public static RedisItemReader<String, String> type() {
		return new RedisItemReader<>(StringCodec.UTF8, ValueType.TYPE);
	}

	public static RedisItemReader<String, String> struct() {
		return struct(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V> struct(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, ValueType.STRUCT);
	}

}
