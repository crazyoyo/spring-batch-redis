package com.redis.spring.batch.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.common.JobExecutionItemStream;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.queue.ConcurrentSetBlockingQueue;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.KeyListener;

import io.lettuce.core.api.StatefulConnection;

public class HotKeyFilter<K, V> extends JobExecutionItemStream implements Predicate<K>, KeyListener<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final Map<KeyWrapper<K>, KeyInfo> keyInfos = new HashMap<>();
	private final Map<KeyWrapper<K>, KeyMetadata<K>> metadatas = new HashMap<>();
	private final GenericObjectPool<StatefulConnection<K, V>> pool;
	private final HotKeyFilterOptions options;
	private final BlockingQueueItemReader<K> reader;
	private final BlockingQueue<K> candidateQueue;

	public HotKeyFilter(GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			HotKeyFilterOptions options) {
		super(jobRunner);
		this.pool = pool;
		this.options = options;
		this.candidateQueue = new ConcurrentSetBlockingQueue<>(options.getCandidateQueueOptions().getCapacity());
		this.reader = new BlockingQueueItemReader<>(candidateQueue,
				options.getCandidateQueueOptions().getPollTimeout());
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public boolean test(K key) {
		KeyWrapper<K> wrapper = new KeyWrapper<>(key);
		if (!metadatas.containsKey(wrapper)) {
			return true;
		}
		KeyMetadata<K> metadata = metadatas.get(wrapper);
		if (!options.getBlockedTypes().contains(metadata.getType())) {
			return true;
		}
		return metadata.getMemoryUsage() < options.getMaxMemoryUsage().toBytes();
	}

	@Override
	protected Job job() {
		ItemWriter<K> writer = new ProcessingItemWriter<>(new KeyMetadataValueReader<>(pool), new MetadataWriter());
		FaultTolerantStepBuilder<K, K> step = jobRunner.step(name, reader, null, writer, options.getStepOptions());
		return jobRunner.job(name).start(step.build()).build();
	}

	private class MetadataWriter implements ItemWriter<KeyMetadata<K>> {

		@Override
		public void write(List<? extends KeyMetadata<K>> items) throws Exception {
			for (KeyMetadata<K> item : items) {
				metadatas.put(new KeyWrapper<>(item.getKey()), item);
			}
		}

	}

	@Override
	public void onDuplicate(K key) {
		KeyInfo info = keyInfos.computeIfAbsent(new KeyWrapper<>(key), w -> new KeyInfo());
		long count = info.incrementCount();
		long duration = info.getDuration();
		if (duration > options.getWindow()) {
			info.reset();
		} else {
			double rate = (double) count * 1000 / duration;
			if (rate > options.getMaxThroughput()) {
				boolean result = candidateQueue.offer(key);
				if (!result) {
					log.warn("Candidate queue is full");
				}
			}
		}
	}

	@Override
	public void close() {
		reader.stop();
		super.close();
	}

	private static class KeyInfo {

		private long startTime = System.currentTimeMillis();
		private long endTime;
		private final AtomicLong count = new AtomicLong();

		public long incrementCount() {
			endTime = System.currentTimeMillis();
			return count.incrementAndGet();
		}

		public long getDuration() {
			return endTime - startTime;
		}

		public void reset() {
			this.startTime = System.currentTimeMillis();
			this.endTime = this.startTime;
			this.count.set(0);
		}

	}

}
