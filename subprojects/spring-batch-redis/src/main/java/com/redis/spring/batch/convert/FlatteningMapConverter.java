package com.redis.spring.batch.convert;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Flattens a nested map using . and [] notation for key names
 */
public class FlatteningMapConverter implements Converter<Map<String, Object>, Map<String, String>>,
		ItemProcessor<Map<String, Object>, Map<String, String>> {

	@Override
	public Map<String, String> process(Map<String, Object> item) {
		return convert(item);
	}

	@Override
	public Map<String, String> convert(Map<String, Object> source) {
		Map<String, String> resultMap = new LinkedHashMap<>();
		flatten("", source.entrySet().iterator(), resultMap);
		return resultMap;
	}

	private void flatten(String prefix, Iterator<? extends Entry<String, Object>> map, Map<String, String> flatMap) {
		String actualPrefix = StringUtils.hasText(prefix) ? prefix.concat(".") : prefix;
		while (map.hasNext()) {
			Entry<String, Object> element = map.next();
			flattenElement(actualPrefix.concat(element.getKey()), element.getValue(), flatMap);
		}
	}

	@SuppressWarnings("unchecked")
	private void flattenElement(String propertyPrefix, @Nullable Object source, Map<String, String> flatMap) {
		if (source == null) {
			return;
		}
		if (source instanceof Iterable) {
			int counter = 0;
			for (Object element : (Iterable<Object>) source) {
				flattenElement(propertyPrefix + "[" + counter + "]", element, flatMap);
				counter++;
			}
		} else if (source instanceof Map) {
			flatten(propertyPrefix, ((Map<String, Object>) source).entrySet().iterator(), flatMap);
		} else {
			flatMap.put(propertyPrefix, String.valueOf(source));
		}
	}

}
