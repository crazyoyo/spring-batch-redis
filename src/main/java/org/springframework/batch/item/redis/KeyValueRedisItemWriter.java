package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.IntrospectedTypeMapper;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
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
	private Converter<T, DataType> itemTypeMapper;
	private Converter<T, Object> itemValueMapper;

	@Builder
	private KeyValueRedisItemWriter(RedisTemplate<K, V> redisTemplate, boolean delete, Converter<T, K> itemKeyMapper,
			Converter<T, DataType> itemTypeMapper, Converter<T, Object> itemValueMapper) {
		super(redisTemplate, delete);
		this.itemKeyMapper = itemKeyMapper;
		this.itemTypeMapper = itemTypeMapper == null ? new IntrospectedTypeMapper<>() : itemTypeMapper;
		this.itemValueMapper = itemValueMapper;
	}

	@Override
	protected void doWrite(List<? extends T> items) {
		redisTemplate.executePipelined(new RedisCallback<Object>() {
			@SuppressWarnings("unchecked")
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (T item : items) {
					byte[] key = ((RedisSerializer<K>) redisTemplate.getKeySerializer())
							.serialize(itemKeyMapper.convert(item));
					DataType type = itemTypeMapper.convert(item);
					Object value = itemValueMapper.convert(item);
					switch (type) {
					case HASH:
						Map<K, V> map = (Map<K, V>) value;
						connection.hMSet(key, byteMap(map));
						break;
					case LIST:
						List<V> list = (List<V>) value;
						byte[][] listArray = new byte[list.size()][];
						for (int index = 0; index < listArray.length; index++) {
							listArray[index] = ((RedisSerializer<V>) redisTemplate.getValueSerializer())
									.serialize(list.get(index));
						}
						connection.lPush(key, listArray);
						break;
					case STREAM:
						for (MapRecord<K, K, V> record : (List<MapRecord<K, K, V>>) value) {
							MapRecord<byte[], byte[], byte[]> byteRecord = MapRecord
									.create(((RedisSerializer<K>) redisTemplate.getKeySerializer())
											.serialize(record.getStream()), byteMap(record.getValue()));
							byteRecord.withId(record.getId());
							connection.xAdd(byteRecord);
						}
						break;
					case ZSET:
						connection.zAdd(key, (Set<Tuple>) value);
						break;
					case SET:
						Set<V> set = (Set<V>) value;
						List<byte[]> setList = new ArrayList<>(set.size());
						for (V element : set) {
							setList.add(((RedisSerializer<V>) redisTemplate.getValueSerializer()).serialize(element));
						}
						connection.sAdd(key, setList.toArray(new byte[setList.size()][]));
						break;
					case STRING:
						connection.set(key,
								((RedisSerializer<V>) redisTemplate.getValueSerializer()).serialize((V) value));
						break;
					case NONE:
						break;
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
