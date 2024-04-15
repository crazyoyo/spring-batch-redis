package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.gen.CollectionOptions;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.Item;
import com.redis.spring.batch.gen.StreamOptions;

class GeneratorTests {

	@Test
	void defaults() throws UnexpectedInputException, ParseException, Exception {
		int count = 123;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(count);
		List<Item> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
	}

	private List<Item> readAll(GeneratorItemReader reader) throws UnexpectedInputException, ParseException, Exception {
		List<Item> list = new ArrayList<>();
		Item ds;
		while ((ds = reader.read()) != null) {
			list.add(ds);
		}
		return list;
	}

	@Test
	void options() throws Exception {
		int count = 123;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(count);
		List<Item> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (Item ds : list) {
			switch (ds.getType()) {
			case SET:
			case LIST:
			case ZSET:
				Assertions.assertEquals(CollectionOptions.DEFAULT_MEMBER_COUNT.getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case STREAM:
				Assertions.assertEquals(StreamOptions.DEFAULT_MESSAGE_COUNT.getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			default:
				break;
			}
		}
	}

	@Test
	void keys() throws Exception {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(10);
		reader.open(new ExecutionContext());
		Item keyValue = reader.read();
		Assertions.assertEquals(GeneratorItemReader.DEFAULT_KEYSPACE + GeneratorItemReader.DEFAULT_KEY_SEPARATOR
				+ GeneratorItemReader.DEFAULT_KEY_RANGE.getMin(), keyValue.getKey());
		String lastKey;
		do {
			lastKey = keyValue.getKey();
		} while ((keyValue = reader.read()) != null);
		Assertions.assertEquals(GeneratorItemReader.DEFAULT_KEYSPACE + GeneratorItemReader.DEFAULT_KEY_SEPARATOR + 10,
				lastKey);
	}

	@Test
	void read() throws Exception {
		int count = 456;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.open(new ExecutionContext());
		reader.setMaxItemCount(456);
		Item ds1 = reader.read();
		assertEquals("gen:1", ds1.getKey());
		int actualCount = 1;
		while (reader.read() != null) {
			actualCount++;
		}
		assertEquals(count, actualCount);
		reader.close();
	}

}
