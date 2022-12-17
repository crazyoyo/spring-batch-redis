package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.awaitility.Awaitility;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;

public class HotKeyFilter<K, V> extends ItemStreamSupport implements Predicate<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final Map<KeyWrapper<K>, KeyContext> metadata = new HashMap<>();
	private final GenericObjectPool<StatefulConnection<K, V>> pool;
	private final HotKeyFilterOptions options;
	private final JobRunner jobRunner;
	private QueueItemReader<K> reader;
	private BlockingQueue<K> candidateQueue;
	private ScheduledExecutorService executor;
	private ScheduledFuture<?> pruneFuture;
	private String name;
	private JobExecution jobExecution;

	public HotKeyFilter(GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			HotKeyFilterOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
		this.pool = pool;
		this.options = options;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		if (jobExecution == null) {
			this.executor = Executors.newSingleThreadScheduledExecutor();
			this.pruneFuture = executor.scheduleAtFixedRate(this::prune, options.getPruneInterval().toMillis(),
					options.getPruneInterval().toMillis(), TimeUnit.MILLISECONDS);
			this.candidateQueue = new LinkedBlockingDeque<>(options.getCandidateQueueOptions().getCapacity());
			this.reader = new QueueItemReader<>(candidateQueue, options.getCandidateQueueOptions().getPollTimeout());
			this.reader.setName(name + "-reader");
			SimpleStepBuilder<K, K> step = jobRunner.step(name, reader, null, new ContextWriter(),
					options.getStepOptions());
			Job job = jobRunner.job(name).start(step.build()).build();
			try {
				this.jobExecution = jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
			} catch (JobExecutionException e) {
				throw new ItemStreamException("Could not start job");
			}
		}
	}

	@Override
	public synchronized void close() {
		if (jobExecution != null) {
			pruneFuture.cancel(true);
			pruneFuture = null;
			executor.shutdown();
			executor = null;
			reader.close();
			Awaitility.await().until(() -> !jobExecution.isRunning());
			jobExecution = null;
		}
		super.close();
	}

	public boolean isOpen() {
		return jobExecution != null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful();
	}

	public Map<K, KeyContext> getMetadata() {
		return metadata.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Entry::getValue));
	}

	/**
	 * Discards metadata entries that are older than 'now - pruneInterval'
	 * 
	 * @return list of keys that have been pruned
	 */
	private List<K> prune() {
		long cutoff = System.currentTimeMillis() - options.getPruneInterval().toMillis();
		List<KeyWrapper<K>> keys = metadata.entrySet().stream().filter(e -> e.getValue().getEndTime() < cutoff)
				.map(Entry::getKey).collect(Collectors.toList());
		keys.forEach(metadata::remove);
		return keys.stream().map(KeyWrapper::getKey).collect(Collectors.toList());
	}

	private class ContextWriter extends AbstractItemStreamItemWriter<K> {

		@SuppressWarnings("unchecked")
		@Override
		public void write(List<? extends K> items) throws Exception {
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				connection.setAutoFlushCommands(false);
				try {
					BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
					List<RedisFuture<String>> typeFutures = new ArrayList<>(items.size());
					List<RedisFuture<Long>> memFutures = new ArrayList<>(items.size());
					for (K key : items) {
						typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
						memFutures.add(((RedisServerAsyncCommands<K, V>) commands).memoryUsage(key));
					}
					connection.flushCommands();
					for (int index = 0; index < items.size(); index++) {
						K key = items.get(index);
						String type = typeFutures.get(index).get(connection.getTimeout().toMillis(),
								TimeUnit.MILLISECONDS);
						Long mem = memFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
						KeyContext context = getContext(key);
						context.setType(type);
						if (mem != null) {
							context.setMemoryUsage(mem);
						}
					}
				} finally {
					connection.setAutoFlushCommands(true);
				}
			}
		}

	}

	public Set<Entry<K, KeyContext>> contexts() {
		return metadata.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Entry::getValue))
				.entrySet();
	}

	@Override
	public boolean test(K key) {
		KeyContext context = getContext(key);
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
		return test(context);
	}

	public boolean test(KeyContext context) {
		if (options.getBlockedTypes().contains(context.getType())) {
			return context.getMemoryUsage() < options.getMaxMemoryUsage().toBytes();
		}
		return true;
	}

	private KeyContext getContext(K key) {
		return metadata.computeIfAbsent(new KeyWrapper<>(key), w -> new KeyContext());
	}

	private static class KeyWrapper<K> {

		protected final K key;

		public KeyWrapper(K name) {
			this.key = name;
		}

		public K getKey() {
			return key;
		}

		@Override
		public int hashCode() {

			if (key instanceof byte[]) {
				return Arrays.hashCode((byte[]) key);
			}
			return key.hashCode();
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof HotKeyFilter.KeyWrapper)) {
				return false;
			}

			KeyWrapper<?> that = (KeyWrapper<?>) obj;

			if (key instanceof byte[] && that.key instanceof byte[]) {
				return Arrays.equals((byte[]) key, (byte[]) that.key);
			}

			return key.equals(that.key);
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append(getClass().getSimpleName());
			sb.append(" [key=").append(key);
			sb.append(']');
			return sb.toString();
		}

	}

	public static class KeyContext {

		private final AtomicLong count = new AtomicLong();
		private String type;
		private long memoryUsage;
		private long startTime = System.currentTimeMillis();
		private long endTime;

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

		public long getEndTime() {
			return endTime;
		}

		public long incrementCount() {
			endTime = System.currentTimeMillis();
			return count.incrementAndGet();
		}

		public long getDuration() {
			return endTime - startTime;
		}

		public long getCount() {
			return count.get();
		}

		public void reset() {
			this.startTime = System.currentTimeMillis();
			this.endTime = this.startTime;
			this.count.set(0);
		}

	}

}
