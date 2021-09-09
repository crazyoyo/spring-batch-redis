package org.springframework.batch.item.redis.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.JacksonJsonNodeReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.util.Map;

@Data
public class Beer {

    private String id;
    private String name;
    private Style style;

    @Data
    public static class Style {

        private long id;
        private String name;

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static JsonItemReader<Map<String, Object>> mapReader() {
        return (JsonItemReader) new JsonItemReaderBuilder<Map>().jsonObjectReader(new JacksonJsonObjectReader<>(Map.class)).name("beer-map-reader").resource(resource()).build();
    }

    public static JsonItemReader<JsonNode> jsonNodeReader() {
        return new JsonItemReaderBuilder<JsonNode>().jsonObjectReader(new JacksonJsonNodeReader()).name("beer-json-node-reader").resource(resource()).build();
    }

    public static JsonItemReader<Beer> reader() {
        return new JsonItemReaderBuilder<Beer>().name("beer-reader").resource(resource()).jsonObjectReader(new JacksonJsonObjectReader<>(new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false), Beer.class)).build();
    }

    private static Resource resource() {
        return new ClassPathResource("beers.json", Beer.class.getClassLoader());
    }

}