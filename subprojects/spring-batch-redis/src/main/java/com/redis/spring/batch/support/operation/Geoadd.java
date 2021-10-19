package com.redis.spring.batch.support.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.convert.ArrayConverter;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Geoadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, GeoValue<V>[]> values;
	private final GeoAddArgs args;

	public Geoadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, GeoValue<V>[]> values,
			GeoAddArgs args) {
		super(key, delete, remove);
		Assert.notNull(values, "A geo-value converter is required");
		this.values = values;
		this.args = args;
	}

	@Override
	protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		return commands.geoadd(key, args, values.convert(item));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		GeoValue<V>[] values = this.values.convert(item);
		if (values == null) {
			return null;
		}
		List<V> members = new ArrayList<>();
		for (GeoValue<V> value : values) {
			members.add(value.getValue());
		}
		return commands.zrem(key, (V[]) members.toArray());
	}

	public static <T> GeoaddValueBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> GeoaddValueBuilder<T> key(Converter<T, String> key) {
		return new GeoaddValueBuilder<>(key);
	}

	public static class GeoaddValueBuilder<T> {

		private final Converter<T, String> key;

		public GeoaddValueBuilder(Converter<T, String> key) {
			this.key = key;
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public GeoaddBuilder<T> values(Converter<T, GeoValue<String>>... values) {
			return new GeoaddBuilder<>(key, (Converter) new ArrayConverter<>(GeoValue.class, values));
		}

		public GeoaddBuilder<T> values(Converter<T, GeoValue<String>[]> values) {
			return new GeoaddBuilder<>(key, values);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class GeoaddBuilder<T> extends RemoveBuilder<T, GeoaddBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, GeoValue<String>[]> values;
		private GeoAddArgs args;

		public GeoaddBuilder(Converter<T, String> key, Converter<T, GeoValue<String>[]> values) {
			super(values);
			this.key = key;
			this.values = values;
		}

		@Override
		public Geoadd<String, String, T> build() {
			return new Geoadd<>(key, del, remove, values, args);
		}

	}

}
