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
import com.redis.spring.batch.reader.DataGeneratorItemReader;
import com.redis.spring.batch.reader.DataGeneratorOptions;

class DataGeneratorTests {

	@Test
	void defaults() throws UnexpectedInputException, ParseException, Exception {
		DataGeneratorOptions options = DataGeneratorOptions.builder().build();
		DataGeneratorItemReader reader = new DataGeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax());
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(DataGeneratorOptions.DEFAULT_KEY_RANGE.getMax(), list.size());
	}

	private List<DataStructure<String>> readAll(DataGeneratorItemReader reader)
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
		DataGeneratorOptions options = DataGeneratorOptions.builder().count(count).build();
		DataGeneratorItemReader reader = new DataGeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax());
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (DataStructure<String> ds : list) {
			switch (ds.getType()) {
			case SET:
				Assertions.assertEquals(DataGeneratorOptions.DEFAULT_SET_OPTIONS.getCardinality().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case LIST:
				Assertions.assertEquals(DataGeneratorOptions.DEFAULT_LIST_OPTIONS.getCardinality().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case ZSET:
				Assertions.assertEquals(DataGeneratorOptions.DEFAULT_ZSET_OPTIONS.getCardinality().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case STREAM:
				Assertions.assertEquals(DataGeneratorOptions.DEFAULT_STREAM_OPTIONS.getMessageCount().getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			default:
				break;
			}
		}
	}

	@Test
	void read() throws Exception {
		DataGeneratorOptions options = DataGeneratorOptions.builder().build();
		DataGeneratorItemReader reader = new DataGeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax());
		DataStructure<String> ds1 = reader.read();
		assertEquals("gen:1", ds1.getKey());
		int count = 1;
		while (reader.read() != null) {
			count++;
		}
		assertEquals(DataGeneratorOptions.DEFAULT_KEY_RANGE.getMax(), count);
	}

}
