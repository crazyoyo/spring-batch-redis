package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.support.RandomDataStructureItemReader;

class RandomDataStructureReaderTests {

	@Test
	void testDefaults() throws UnexpectedInputException, ParseException, Exception {
		RandomDataStructureItemReader reader = RandomDataStructureItemReader.builder().build();
		List<DataStructure<String>> list = readAll(reader);
		long expectedCount = RandomDataStructureItemReader.DEFAULT_SEQUENCE.getMaximum()
				- RandomDataStructureItemReader.DEFAULT_SEQUENCE.getMinimum();
		Assertions.assertEquals(expectedCount, list.size());
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

	@SuppressWarnings("incomplete-switch")
	@Test
	void testOptions() throws Exception {
		int count = 123;
		RandomDataStructureItemReader reader = RandomDataStructureItemReader.builder().end(count).build();
		List<DataStructure<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (DataStructure<String> ds : list) {
			switch (Type.of(ds.getType())) {
			case SET:
			case LIST:
			case ZSET:
			case STREAM:
				Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY.getMinimum(),
						((Collection<?>) ds.getValue()).size());
				break;
			}
		}
	}

}
