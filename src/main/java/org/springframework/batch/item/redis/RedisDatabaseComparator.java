package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader.KeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.Transfer;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;

@Builder
public class RedisDatabaseComparator implements ItemWriter<DataStructure> {

    private RedisDataStructureItemReader sourceReader;
    private RedisDataStructureItemReader targetReader;
    @Default
    private int batchSize = KeyValueItemReaderBuilder.DEFAULT_BATCH_SIZE;
    @Default
    private int threads = 1;
    @Default
    private long ttlTolerance = 1;
    @Getter
    private final List<String> ok = new ArrayList<>();
    @Getter
    private final List<String> badValues = new ArrayList<>();
    @Getter
    private final List<String> missingKeys = new ArrayList<>();
    @Getter
    private final List<String> extraKeys = new ArrayList<>();
    @Getter
    private final List<String> badTtls = new ArrayList<>();

    public CompletableFuture<Void> executeAsync() {
	return Transfer.<DataStructure, DataStructure>builder().reader(sourceReader).writer(this).batch(batchSize)
		.threads(threads).build().executeAsync();
    }

    @Override
    public void write(List<? extends DataStructure> items) throws Exception {
	List<String> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
	List<DataStructure> targetItems = targetReader.read(keys);
	for (int index = 0; index < items.size(); index++) {
	    DataStructure sourceItem = items.get(index);
	    DataStructure targetItem = targetItems.get(index);
	    compare(sourceItem, targetItem).add(sourceItem.getKey());
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
