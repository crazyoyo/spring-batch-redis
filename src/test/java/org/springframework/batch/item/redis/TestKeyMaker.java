package org.springframework.batch.item.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.KeyMaker;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.Map;

public class TestKeyMaker {

    @SuppressWarnings("unchecked")
    @Test
    public void testSingleKeyConverter() {
        String prefix = "beer";
        String idField = "id";
        KeyMaker<Map<String, String>> keyMaker = KeyMaker.<Map<String, String>>builder().prefix(prefix).converters(m -> m.get(idField)).build();
        Map<String, String> map = new HashMap<>();
        String id = "123";
        map.put(idField, id);
        map.put("name", "La fin du monde");
        String key = keyMaker.convert(map);
        Assertions.assertEquals(prefix + KeyMaker.DEFAULT_SEPARATOR + id, key);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiKeyConverter() {
        String prefix = "inventory";
        Converter<Map<String, String>, String> storeExtractor = m -> m.get("store");
        Converter<Map<String, String>, String> skuExtractor = m -> m.get("sku");
        Map<String, String> map = new HashMap<>();
        String store = "403";
        String sku = "39323";
        map.put("store", store);
        map.put("sku", sku);
        map.put("name", "La fin du monde");
        Assertions.assertEquals(prefix + KeyMaker.DEFAULT_SEPARATOR + store + KeyMaker.DEFAULT_SEPARATOR + sku, KeyMaker.<Map<String, String>>builder().prefix(prefix).converters(storeExtractor, skuExtractor).build().convert(map));
        String separator = "~][]:''~";
        Assertions.assertEquals(prefix + separator + store + separator + sku, KeyMaker.<Map<String, String>>builder().prefix(prefix).separator(separator).converters(storeExtractor, skuExtractor).build().convert(map));
    }

}
