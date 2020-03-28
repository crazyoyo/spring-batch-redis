package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.data.AbstractPaginatedDataItemReader;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisItemReader<K, V, T> extends AbstractPaginatedDataItemReader<T> {

	protected @Setter RedisTemplate<K, V> redisTemplate;
	private @Setter ScanOptions scanOptions;
	private @Setter int batchSize;

	private Cursor<byte[]> cursor;

	public AbstractRedisItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	protected AbstractRedisItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			RedisTemplate<K, V> redisTemplate, ScanOptions scanOptions, int batchSize) {
		setCurrentItemCount(currentItemCount);
		setMaxItemCount(maxItemCount == null ? Integer.MAX_VALUE : maxItemCount);
		setSaveState(saveState == null ? true : saveState);
		Assert.state(redisTemplate != null, "An instance of RedisTemplate is required.");
		Assert.state(scanOptions != null, "An instance of ScanOptions is required.");
		Assert.state(batchSize > 0, "Batch size must be greater than 0.");
		this.redisTemplate = redisTemplate;
		this.scanOptions = scanOptions;
		this.batchSize = batchSize;

	}

	@Override
	protected Iterator<T> doPageRead() {
		List<byte[]> keys = new ArrayList<>();
		try {
			while (keys.size() < batchSize && cursor.hasNext()) {
				keys.add(cursor.next());
			}
		} catch (Exception e) {
			log.error("Could not read keys", e);
		}
		return getValues(keys).iterator();
	}

	protected abstract List<T> getValues(List<byte[]> keys);

	@Override
	protected void doOpen() throws Exception {
		this.cursor = redisTemplate.getConnectionFactory().getConnection().scan(scanOptions);
	}

	@Override
	protected void doClose() throws Exception {
		this.cursor.close();
		this.cursor = null;
		log.info("Items read: {}", getCurrentItemCount());
	}

}
