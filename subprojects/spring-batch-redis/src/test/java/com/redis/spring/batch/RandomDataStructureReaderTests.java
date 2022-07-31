package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader;

class RandomDataStructureReaderTests {

	@Test
	void defaults() throws UnexpectedInputException, ParseException, Exception {
		DataStructureGeneratorItemReader reader = DataStructureGeneratorItemReader.builder().build();
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(DataStructureGeneratorItemReader.DEFAULT_MAX_ITEM_COUNT, list.size());
	}

	private List<DataStructure<String>> readAll(DataStructureGeneratorItemReader reader)
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
		DataStructureGeneratorItemReader reader = DataStructureGeneratorItemReader.builder().maxItemCount(count)
				.build();
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (DataStructure<String> ds : list) {
			if (ds.getType() == Type.SET || ds.getType() == Type.LIST || ds.getType() == Type.ZSET
					|| ds.getType() == Type.STREAM) {
				Assertions.assertEquals(DataStructureGeneratorItemReader.DEFAULT_SET_SIZE.getMin(),
						((Collection<?>) ds.getValue()).size());
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void read() throws Exception {
		DataStructureGeneratorItemReader reader = DataStructureGeneratorItemReader.builder().build();
		DataStructure<String> ds1 = reader.read();
		assertEquals(DataStructureGeneratorItemReader.defaultTypes().get(0), ds1.getType());
		assertEquals("gen:1", ds1.getKey());
		assertEquals(DataStructureGeneratorItemReader.DEFAULT_HASH_SIZE.getMin(),
				((Map<String, Object>) ds1.getValue()).size());
		int count = 1;
		while (reader.read() != null) {
			count++;
		}
		assertEquals(DataStructureGeneratorItemReader.DEFAULT_MAX_ITEM_COUNT, count);
	}

}
