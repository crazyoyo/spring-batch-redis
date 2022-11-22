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
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.reader.DataGeneratorItemReader;
import com.redis.spring.batch.reader.DataGeneratorOptions;

class DataStructureGeneratorTests {

	@Test
	void defaults() throws UnexpectedInputException, ParseException, Exception {
		DataGeneratorOptions options = DataGeneratorOptions.builder().build();
		List<DataStructure<String>> list = readAll(new DataGeneratorItemReader(options));
		Assertions.assertEquals(DataGeneratorOptions.DEFAULT_RANGE.getMax(), list.size());
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
		List<DataStructure<String>> list = readAll(new DataGeneratorItemReader(options));
		Assertions.assertEquals(count, list.size());
		for (DataStructure<String> ds : list) {
			if (ds.getType() == Type.SET || ds.getType() == Type.LIST || ds.getType() == Type.ZSET
					|| ds.getType() == Type.STREAM) {
				Assertions.assertEquals(DataGeneratorOptions.DEFAULT_SET_OPTIONS.getMemberCount().getMin(),
						((Collection<?>) ds.getValue()).size());
			}
		}
	}

	@Test
	void read() throws Exception {
		DataGeneratorItemReader reader = new DataGeneratorItemReader(DataGeneratorOptions.builder().build());
		DataStructure<String> ds1 = reader.read();
		assertEquals("gen:1", ds1.getKey());
		int count = 1;
		while (reader.read() != null) {
			count++;
		}
		assertEquals(DataGeneratorOptions.DEFAULT_RANGE.getMax(), count);
	}

}
