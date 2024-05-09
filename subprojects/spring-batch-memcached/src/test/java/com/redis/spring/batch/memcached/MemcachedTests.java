package com.redis.spring.batch.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.ClassUtils;
import org.testcontainers.utility.DockerImageName;

import com.redis.spring.batch.memcached.common.ByteArrayTranscoder;
import com.redis.spring.batch.memcached.gen.MemcachedGeneratorItemReader;
import com.redis.spring.batch.memcached.gen.Range;
import com.redis.testcontainers.MemcachedContainer;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

@TestInstance(Lifecycle.PER_CLASS)
public class MemcachedTests {

	private static final DockerImageName imageName = MemcachedContainer.DEFAULT_IMAGE_NAME
			.withTag(MemcachedContainer.DEFAULT_TAG);

	private static final MemcachedContainer source = new MemcachedContainer(imageName);
	private static final MemcachedContainer target = new MemcachedContainer(imageName);

	private Supplier<MemcachedClient> clientSupplier;
	private MemcachedClient client;
	private Supplier<MemcachedClient> targetClientSupplier;
	private MemcachedClient targetClient;

	private static final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

	public static String name(TestInfo info) {
		StringBuilder displayName = new StringBuilder(info.getDisplayName().replace("(TestInfo)", ""));
		info.getTestClass().ifPresent(c -> displayName.append("-").append(ClassUtils.getShortName(c)));
		return displayName.toString();
	}

	public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
		List<T> list = new ArrayList<>();
		T element;
		while ((element = reader.read()) != null) {
			list.add(element);
		}
		return list;
	}

	@BeforeAll
	void setup() throws IOException, InterruptedException {
		source.start();
		target.start();
		clientSupplier = () -> client(source.getMemcachedAddresses());
		client = clientSupplier.get();
		targetClientSupplier = () -> client(target.getMemcachedAddresses());
		targetClient = targetClientSupplier.get();
	}

	private static MemcachedClient client(List<InetSocketAddress> addresses) {
		try {
			return new MemcachedClient(addresses);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@AfterAll
	void teardown() {
		if (client != null) {
			client.shutdown();
		}
		if (source != null) {
			source.stop();
		}
		if (targetClient != null) {
			targetClient.shutdown();
		}
		if (target != null) {
			target.stop();
		}
	}

	@BeforeEach
	void flushall() {
		client.flush();
		targetClient.flush();
	}

	@Test
	void writer() throws Exception {
		int count = 321;
		MemcachedGeneratorItemReader generator = new MemcachedGeneratorItemReader();
		generator.setMaxItemCount(count);
		generator.open(new ExecutionContext());
		List<MemcachedEntry> entries = readAll(generator);
		generator.close();
		write(entries);
		for (MemcachedEntry entry : entries) {
			MemcachedEntry actual = new MemcachedEntry();
			actual.setKey(entry.getKey());
			actual.setValue(client.get(entry.getKey(), transcoder));
			actual.setExpiration(-1);
			assertEquals(entry, actual);
		}
	}

	@Test
	void reader(TestInfo info) throws Exception {
		int count = 4321;
		MemcachedGeneratorItemReader generator = new MemcachedGeneratorItemReader();
		generator.setMaxItemCount(count);
		generator.open(new ExecutionContext());
		List<MemcachedEntry> entries = readAll(generator);
		generator.close();
		write(entries);
		MemcachedItemReader reader = new MemcachedItemReader(clientSupplier);
		reader.setName(name(info));
		try {
			reader.open(new ExecutionContext());
			List<MemcachedEntry> readEntries = readAll(reader);
			Map<String, MemcachedEntry> readEntryMap = readEntries.stream()
					.collect(Collectors.toMap(MemcachedEntry::getKey, Function.identity()));
			Assertions.assertEquals(count, readEntries.size());
			for (MemcachedEntry entry : entries) {
				MemcachedEntry readEntry = readEntryMap.get(entry.getKey());
				assertEquals(entry, readEntry);
			}
		} finally {
			reader.close();
		}
	}

	@Test
	void readerExpiration(TestInfo info) throws Exception {
		int count = 4321;
		MemcachedGeneratorItemReader generator = new MemcachedGeneratorItemReader();
		generator.setMaxItemCount(count);
		generator.setExpiration(Range.of(10000, 20000));
		generator.open(new ExecutionContext());
		List<MemcachedEntry> entries = readAll(generator);
		generator.close();
		write(entries);
		MemcachedItemReader reader = new MemcachedItemReader(clientSupplier);
		reader.setName(name(info));
		try {
			reader.open(new ExecutionContext());
			List<MemcachedEntry> readEntries = readAll(reader);
			Map<String, MemcachedEntry> readEntryMap = readEntries.stream()
					.collect(Collectors.toMap(MemcachedEntry::getKey, Function.identity()));
			Assertions.assertEquals(count, readEntries.size());
			for (MemcachedEntry entry : entries) {
				MemcachedEntry readEntry = readEntryMap.get(entry.getKey());
				assertEquals(entry, readEntry);
			}
		} finally {
			reader.close();
		}
	}

	private static void assertEquals(MemcachedEntry expected, MemcachedEntry actual) {
		Assertions.assertEquals(expected.getKey(), actual.getKey());
		Assertions.assertArrayEquals(expected.getValue(), actual.getValue());
		if (expected.getExpiration() == 0) {
			Assertions.assertEquals(-1, actual.getExpiration());
		} else {
			Assertions.assertEquals(expected.getExpiration(), actual.getExpiration());
		}
	}

	private void write(List<MemcachedEntry> entries) throws Exception {
		MemcachedItemWriter writer = new MemcachedItemWriter(clientSupplier);
		try {
			writer.open(new ExecutionContext());
			writer.write(new Chunk<>(entries));
		} finally {
			writer.close();
		}
	}

}
