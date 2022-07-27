package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.reader.RandomDataStructureItemReader;

class RandomDataStructureReaderTests {

	@Test
	void testDefaults() throws UnexpectedInputException, ParseException, Exception {
		RandomDataStructureItemReader reader = RandomDataStructureItemReader.builder().build();
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COUNT, list.size());
	}

	private List<DataStructure<String>> readAll(RandomDataStructureItemReader reader)
			throws UnexpectedInputException, ParseException, Exception {
		List<DataStructure<String>> list = new ArrayList<>();
		DataStructure<String> ds;
		while ((ds = reader.read()) != null) {
			list.add(ds);
		}
		return list;
	}

	@Test
	void testOptions() throws Exception {
		int count = 123;
		RandomDataStructureItemReader reader = RandomDataStructureItemReader.builder().count(count).build();
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (DataStructure<String> ds : list) {
			Type type = Type.of(ds.getType());
			if (type == null) {
				continue;
			}
			if (type == Type.SET || type == Type.LIST || type == Type.ZSET || type == Type.STREAM) {
				Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY.getMinimum(),
						((Collection<?>) ds.getValue()).size());
			}
		}
	}

}
