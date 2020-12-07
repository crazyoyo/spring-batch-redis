package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.redis.support.TransferOptions;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
public class DatabaseComparator {

	private final DataStructureItemReader left;
	private final DataStructureReader right;
	private final TransferOptions transferOptions;
	/**
	 * TTL tolerance in seconds
	 */
	private final long ttlTolerance;
	private final List<String> ok = new ArrayList<>();
	private final List<String> badValues = new ArrayList<>();
	private final List<String> missingKeys = new ArrayList<>();
	private final List<String> extraKeys = new ArrayList<>();
	private final List<String> badTtls = new ArrayList<>();

	public DatabaseComparator(AbstractRedisClient leftClient,
			GenericObjectPoolConfig<StatefulConnection<String, String>> leftPoolConfig, ReaderOptions leftOptions,
			AbstractRedisClient rightClient,
			GenericObjectPoolConfig<StatefulConnection<String, String>> rightPoolConfig,
			TransferOptions transferOptions, long ttlTolerance) {
		Assert.notNull(leftClient, "Left Redis client is required.");
		Assert.notNull(leftPoolConfig, "Left connection pool config is required.");
		Assert.notNull(rightClient, "Right Redis client is required.");
		Assert.notNull(rightPoolConfig, "Right connection pool config is required.");
		Assert.notNull(transferOptions, "Transfer options are required.");
		this.left = DataStructureItemReader.builder(leftClient).poolConfig(leftPoolConfig).options(leftOptions).build();
		this.right = DataStructureReader.builder(rightClient).poolConfig(rightPoolConfig).build();
		this.transferOptions = transferOptions;
		this.ttlTolerance = ttlTolerance;
	}

	public TransferExecution<DataStructure, DataStructure> execution() {
		Transfer<DataStructure, DataStructure> transfer = Transfer.<DataStructure, DataStructure>builder()
				.name("comparator").reader(left).writer(new ComparatorWriter()).options(transferOptions).build();
		return new TransferExecution<>(transfer);
	}

	private class ComparatorWriter implements ItemStreamWriter<DataStructure> {

		@Override
		public void open(ExecutionContext executionContext) throws ItemStreamException {
			log.info("Opening right");
			right.open(executionContext);
		}

		@Override
		public void update(ExecutionContext executionContext) throws ItemStreamException {
			log.info("Updating right");
			right.update(executionContext);
		}

		@Override
		public void close() throws ItemStreamException {
			log.info("Closing right");
			right.close();
		}

		@Override
		public void write(List<? extends DataStructure> items) throws Exception {
			List<String> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
			List<DataStructure> targetItems = right.read(keys);
			for (int index = 0; index < items.size(); index++) {
				DataStructure sourceItem = items.get(index);
				DataStructure targetItem = targetItems.get(index);
				compare(sourceItem, targetItem).add(sourceItem.getKey());
			}
		}
	}

	private List<String> compare(DataStructure source, DataStructure target) {
		if (source.getValue() == null) {
			if (target.getValue() == null) {
				return ok;
			}
			return extraKeys;
		}
		if (target.getValue() == null) {
			return missingKeys;
		}
		if (Objects.deepEquals(source.getValue(), target.getValue())) {
			long ttlDiff = Math.abs(source.getTtl() - target.getTtl());
			if (ttlDiff > ttlTolerance) {
				return badTtls;
			}
			return ok;
		}
		return badValues;
	}

	public static DatabaseComparatorBuilder builder(AbstractRedisClient leftClient, AbstractRedisClient rightClient) {
		return new DatabaseComparatorBuilder(leftClient, rightClient);
	}

	@Setter
	@Accessors(fluent = true)
	public static class DatabaseComparatorBuilder {
		public static final long DEFAULT_TTL_TOLERANCE = 1;

		private final AbstractRedisClient leftClient;
		private final AbstractRedisClient rightClient;
		private GenericObjectPoolConfig<StatefulConnection<String, String>> leftPoolConfig = new GenericObjectPoolConfig<>();
		private ReaderOptions leftOptions = ReaderOptions.builder().build();
		private GenericObjectPoolConfig<StatefulConnection<String, String>> rightPoolConfig = new GenericObjectPoolConfig<>();
		private TransferOptions transferOptions = TransferOptions.builder().build();
		private long ttlTolerance = DEFAULT_TTL_TOLERANCE;

		public DatabaseComparatorBuilder(AbstractRedisClient leftClient, AbstractRedisClient rightClient) {
			this.leftClient = leftClient;
			this.rightClient = rightClient;
		}

		public DatabaseComparator build() {
			return new DatabaseComparator(leftClient, leftPoolConfig, leftOptions, rightClient, rightPoolConfig,
					transferOptions, ttlTolerance);
		}

	}

}
