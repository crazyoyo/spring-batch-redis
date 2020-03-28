package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;

import lombok.Builder;

public class KeyValueRedisItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	private Converter<T, K> itemKeyMapper;
	private Converter<T, Object> itemValueMapper;

	@Builder
	private KeyValueRedisItemWriter(RedisTemplate<K, V> redisTemplate, boolean delete, Converter<T, K> itemKeyMapper,
			Converter<T, Object> itemValueMapper) {
		super(redisTemplate, delete);
		this.itemKeyMapper = itemKeyMapper;
		this.itemValueMapper = itemValueMapper;
	}

	@Override
	protected void doWrite(List<? extends T> items) {
		redisTemplate.executePipelined(new RedisCallback<Object>() {
			@SuppressWarnings("unchecked")
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<K> keySerializer = (RedisSerializer<K>) redisTemplate.getKeySerializer();
				RedisSerializer<V> valueSerializer = (RedisSerializer<V>) redisTemplate.getValueSerializer();
				for (T item : items) {
					byte[] key = keySerializer.serialize(itemKeyMapper.convert(item));
					Object value = itemValueMapper.convert(item);
					if (value instanceof Map) {
						Map<K, V> map = (Map<K, V>) value;
						connection.hMSet(key, byteMap(map));
					} else if (value instanceof List) {
						if (((List<?>) value).isEmpty()) {
							continue;
						}
						if (((List<?>) value).get(0) instanceof MapRecord) {
							for (MapRecord<K, K, V> record : (List<MapRecord<K, K, V>>) value) {
								MapRecord<byte[], byte[], byte[]> byteRecord = MapRecord.create(
										keySerializer.serialize(record.getStream()), byteMap(record.getValue()));
								byteRecord.withId(record.getId());
								connection.xAdd(byteRecord);
							}
						} else {
							List<V> list = (List<V>) value;
							byte[][] listArray = new byte[list.size()][];
							for (int index = 0; index < listArray.length; index++) {
								listArray[index] = valueSerializer.serialize(list.get(index));
							}
							connection.lPush(key, listArray);
						}
					} else if (value instanceof Set) {
						if (((Set<?>) value).isEmpty()) {
							continue;
						}
						if (((Set<?>) value).iterator().next() instanceof Tuple) {
							connection.zAdd(key, (Set<Tuple>) value);
						} else {
							Set<V> set = (Set<V>) value;
							List<byte[]> setList = new ArrayList<>(set.size());
							for (V element : set) {
								setList.add(valueSerializer.serialize(element));
							}
							connection.sAdd(key, setList.toArray(new byte[setList.size()][]));
						}
					} else {
						connection.set(key, valueSerializer.serialize((V) value));
					}
				}
				return null;
			}

		});

	}

	@SuppressWarnings("unchecked")
	private Map<byte[], byte[]> byteMap(Map<K, V> map) {
		RedisSerializer<K> keySerializer = (RedisSerializer<K>) redisTemplate.getKeySerializer();
		RedisSerializer<V> valueSerializer = (RedisSerializer<V>) redisTemplate.getValueSerializer();
		Map<byte[], byte[]> byteMap = new HashMap<>(map.size());
		map.forEach((k, v) -> byteMap.put(keySerializer.serialize(k), valueSerializer.serialize(v)));
		return byteMap;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(itemKeyMapper, "itemKeyMapper required.");
		Assert.notNull(itemValueMapper, "itemValueMapper required.");
	}

	@Override
	protected Collection<K> getKeys(List<? extends T> items) {
		return items.stream().map(itemKeyMapper::convert).collect(Collectors.toList());
	}

}
