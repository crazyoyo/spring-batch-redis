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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.common.JobExecutionItemStream;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.queue.ConcurrentSetBlockingQueue;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.Listener;

import io.lettuce.core.api.StatefulConnection;

public class HotKeyFilter<K, V> extends JobExecutionItemStream implements Predicate<K>, Listener<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final Map<KeyWrapper<K>, KeyContext<K>> metadata = new HashMap<>();
	private final HotKeyFilterOptions options;
	private final QueueItemReader<K> reader;
	private final KeyContextWriter<K, V> writer;
	private final BlockingQueue<K> candidateQueue;
	private ScheduledExecutorService executor;
	private ScheduledFuture<?> pruneFuture;

	public HotKeyFilter(GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			HotKeyFilterOptions options) {
		super(jobRunner);
		this.options = options;
		this.candidateQueue = new ConcurrentSetBlockingQueue<>(options.getCandidateQueueOptions().getCapacity());
		this.reader = new QueueItemReader<>(candidateQueue, options.getCandidateQueueOptions().getPollTimeout());
		this.writer = new KeyContextWriter<>(pool);
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

	@Override
	public boolean isOpen() {
		return super.isOpen() && reader.isOpen();
	}

	public Map<K, KeyContext<K>> getMetadata() {
		return metadata.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Entry::getValue));
	}

	/**
	 * Discards metadata entries that are older than 'now - pruneInterval'
	 * 
	 * @return list of keys that have been pruned
	 */
	private List<KeyWrapper<K>> prune() {
		long cutoff = System.currentTimeMillis() - options.getPruneInterval().toMillis();
		List<KeyWrapper<K>> keys = metadata.entrySet().stream().filter(e -> e.getValue().getEndTime() < cutoff)
				.map(Entry::getKey).collect(Collectors.toList());
		keys.forEach(metadata::remove);
		return keys;
	}

	@Override
	protected Job job() {
		SimpleStepBuilder<K, KeyContext<K>> step = jobRunner.step(name, reader, this::getKeyContext, writer,
				options.getStepOptions());
		return jobRunner.job(name).start(step.build()).build();
	}

	@Override
	public boolean test(K key) {
		KeyWrapper<K> wrapper = new KeyWrapper<>(key);
		if (!metadata.containsKey(wrapper)) {
			return true;
		}
		KeyContext<K> info = metadata.get(wrapper);
		if (!options.getBlockedTypes().contains(info.getType())) {
			return true;
		}
		return info.getMemoryUsage() < options.getMaxMemoryUsage().toBytes();
	}

	private KeyContext<K> getKeyContext(K key) {
		return metadata.computeIfAbsent(new KeyWrapper<>(key), w -> new KeyContext<>(key));
	}

	@Override
	public void onAccept(K key) {
		// do nothing
	}

	@Override
	public void onKey(K key) {
		// do nothing
	}

	@Override
	public void onReject(K key) {
		// do nothing
	}

	@Override
	public void onDuplicate(K key) {
		KeyContext<K> context = getKeyContext(key);
		long count = context.incrementCount();
		long duration = context.getDuration();
		if (duration > options.getWindow()) {
			context.reset();
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

}
