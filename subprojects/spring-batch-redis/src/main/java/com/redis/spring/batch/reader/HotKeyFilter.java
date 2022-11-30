package com.redis.spring.batch.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
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
	private final GenericObjectPool<StatefulConnection<K, V>> pool;
	private final HotKeyFilterOptions options;
	private final QueueItemReader<K> reader;
	private final BlockingQueue<K> candidateQueue;
	private ScheduledExecutorService executor;
	private ScheduledFuture<?> pruneFuture;

	public HotKeyFilter(GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			HotKeyFilterOptions options) {
		super(jobRunner);
		this.pool = pool;
		this.options = options;
		this.candidateQueue = new ConcurrentSetBlockingQueue<>(options.getCandidateQueueOptions().getCapacity());
		this.reader = new QueueItemReader<>(candidateQueue, options.getCandidateQueueOptions().getPollTimeout());
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.pruneFuture = executor.scheduleAtFixedRate(this::prune, options.getPruneInterval().toMillis(),
				options.getPruneInterval().toMillis(), TimeUnit.MILLISECONDS);
	}

	public Map<K, KeyInfo> getKeyInfos() {
		return keyInfos.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Entry::getValue));
	}

	/**
	 * Discards KeyInfo entries that are older than 'now - pruneInterval'
	 * 
	 * @return list of keys that have been pruned
	 */
	private List<KeyWrapper<K>> prune() {
		long cutoff = System.currentTimeMillis() - options.getPruneInterval().toMillis();
		List<KeyWrapper<K>> keys = keyInfos.entrySet().stream().filter(e -> e.getValue().getEndTime() < cutoff)
				.map(Entry::getKey).collect(Collectors.toList());
		keys.forEach(keyInfos::remove);
		return keys;
	}

	@Override
	protected Job job() {
		ItemWriter<K> writer = new ProcessingItemWriter<>(new KeyMetadataValueReader<>(pool), new MetadataWriter());
		SimpleStepBuilder<K, K> step = jobRunner.step(name, reader, null, writer, options.getStepOptions());
		return jobRunner.job(name).start(step.build()).build();
	}

	@Override
	public boolean test(K key) {
		KeyWrapper<K> wrapper = new KeyWrapper<>(key);
		if (!keyInfos.containsKey(wrapper)) {
			return true;
		}
		KeyInfo metadata = keyInfos.get(wrapper);
		if (!options.getBlockedTypes().contains(metadata.getType())) {
			return true;
		}
		return metadata.getMemoryUsage() < options.getMaxMemoryUsage().toBytes();
	}

	private class MetadataWriter implements ItemWriter<KeyMetadata<K>> {

		@Override
		public void write(List<? extends KeyMetadata<K>> items) throws Exception {
			for (KeyMetadata<K> item : items) {
				KeyInfo info = getKeyInfo(item.getKey());
				info.setMemoryUsage(item.getMemoryUsage());
				info.setType(item.getType());
			}
		}

	}

	private KeyInfo getKeyInfo(K key) {
		return keyInfos.computeIfAbsent(new KeyWrapper<>(key), w -> new KeyInfo());
	}

	@Override
	public void onDuplicate(K key) {
		KeyInfo info = getKeyInfo(key);
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
		pruneFuture.cancel(true);
		executor.shutdown();
		super.close();
	}

	private static class KeyInfo {

		private final AtomicLong count = new AtomicLong();
		private long startTime = System.currentTimeMillis();
		private long endTime;
		private String type;
		private long memoryUsage;

		public long getEndTime() {
			return endTime;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public long getMemoryUsage() {
			return memoryUsage;
		}

		public void setMemoryUsage(long memoryUsage) {
			this.memoryUsage = memoryUsage;
		}

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
