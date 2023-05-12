package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;

class GeneratorTests {

	@Test
	void defaults() throws UnexpectedInputException, ParseException, Exception {
		GeneratorReaderOptions options = GeneratorReaderOptions.builder().build();
		GeneratorItemReader reader = new GeneratorItemReader(options);
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(GeneratorReaderOptions.DEFAULT_KEY_RANGE.getMax(), list.size());
	}

	private List<DataStructure<String>> readAll(GeneratorItemReader reader)
			throws UnexpectedInputException, ParseException, Exception {
		List<DataStructure<String>> list = new ArrayList<>();
		DataStructure<String> ds;
		while ((ds = reader.read()) != null) {
			list.add(ds);
		}
		return list;
	}

	@Test
	void options() throws Exception {
		int count = 123;
		GeneratorReaderOptions options = GeneratorReaderOptions.builder().count(count).build();
		GeneratorItemReader reader = new GeneratorItemReader(options);
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (DataStructure<String> ds : list) {
			switch (ds.getType()) {
			case DataStructure.SET:
				Assertions.assertEquals(GeneratorReaderOptions.DEFAULT_SET_OPTIONS.getCardinality().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case DataStructure.LIST:
				Assertions.assertEquals(GeneratorReaderOptions.DEFAULT_LIST_OPTIONS.getCardinality().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case DataStructure.ZSET:
				Assertions.assertEquals(GeneratorReaderOptions.DEFAULT_ZSET_OPTIONS.getCardinality().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case DataStructure.STREAM:
				Assertions.assertEquals(GeneratorReaderOptions.DEFAULT_STREAM_OPTIONS.getMessageCount().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			default:
				break;
			}
		}
	}

	@Test
	void read() throws Exception {
		GeneratorReaderOptions options = GeneratorReaderOptions.builder().build();
		GeneratorItemReader reader = new GeneratorItemReader(options);
		DataStructure<String> ds1 = reader.read();
		assertEquals("gen:1", ds1.getKey());
		int count = 1;
		while (reader.read() != null) {
			count++;
		}
		assertEquals(GeneratorReaderOptions.DEFAULT_KEY_RANGE.getMax(), count);
	}

}
