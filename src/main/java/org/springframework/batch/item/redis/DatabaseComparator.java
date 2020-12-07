package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.redis.support.TransferOptions;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
@Builder
public class DatabaseComparator {

	private static final long DEFAULT_TTL_TOLERANCE = 1;

	private final DataStructureItemReader left;
	private final DataStructureReader right;
	@Default
	private final TransferOptions transferOptions = TransferOptions.builder().build();
	/**
	 * TTL tolerance in seconds
	 */
	@Default
	private final long ttlTolerance = DEFAULT_TTL_TOLERANCE;
	private final List<String> ok = new ArrayList<>();
	private final List<String> badValues = new ArrayList<>();
	private final List<String> missingKeys = new ArrayList<>();
	private final List<String> extraKeys = new ArrayList<>();
	private final List<String> badTtls = new ArrayList<>();

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

}
