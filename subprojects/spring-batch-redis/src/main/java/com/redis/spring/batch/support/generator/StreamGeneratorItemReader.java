package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.support.DataStructure.Type;

import io.lettuce.core.StreamMessage;

public class StreamGeneratorItemReader
		extends CollectionGeneratorItemReader<Collection<StreamMessage<String, String>>> {

	private static final Logger log = LoggerFactory.getLogger(StreamGeneratorItemReader.class);

	public StreamGeneratorItemReader() {
		super(Type.STREAM);
	}

	@Override
	protected Collection<StreamMessage<String, String>> value() {
		String stream = "stream:" + index();
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < cardinality(); elementIndex++) {
			messages.add(new StreamMessage<>(stream, null, map()));
		}
		log.debug("Generated {} stream messages", messages.size());
		return messages;
	}

}
