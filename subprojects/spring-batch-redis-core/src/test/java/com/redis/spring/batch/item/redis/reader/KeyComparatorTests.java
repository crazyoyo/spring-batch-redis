package com.redis.spring.batch.item.redis.reader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;

class KeyComparatorTests {

	@Test
	void testStreamMessageId() {
		DefaultKeyComparator<String, String> comparator = new DefaultKeyComparator<>(StringCodec.UTF8);
		String key = "key:1";
		String type = "stream";
		String messageId = "12345";
		Map<String, String> body = new HashMap<>();
		KeyValue<String, Object> kv1 = new KeyValue<>();
		kv1.setKey(key);
		kv1.setType(type);
		kv1.setValue(Arrays.asList(new StreamMessage<>(key, messageId, body)));
		KeyValue<String, Object> kv2 = new KeyValue<>();
		kv2.setKey(key);
		kv2.setType(type);
		StreamMessage<String, String> message2 = new StreamMessage<>(key, messageId + "1", body);
		kv2.setValue(Arrays.asList(message2));
		Assertions.assertEquals(Status.VALUE, comparator.compare(kv1, kv2).getStatus());
		comparator.setIgnoreStreamMessageId(true);
		Assertions.assertEquals(Status.OK, comparator.compare(kv1, kv2).getStatus());
	}

}
