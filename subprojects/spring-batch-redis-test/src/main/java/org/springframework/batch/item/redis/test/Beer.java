package org.springframework.batch.item.redis.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.CreateOptions;
import com.redis.lettucemod.api.search.Field;
import lombok.Data;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.JacksonJsonNodeReader;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.batch.item.redis.support.convert.MapFlattener;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.util.Map;

@Data
public class Beer {

    public static final String INDEX = "beers";
    public static final String PREFIX = "beer:";

    public static final Field ID = Field.tag("id").sortable().build();
    public static final Field NAME = Field.text("name").sortable().build();
    public static final Field STYLE = Field.text("style").matcher(com.redis.lettucemod.api.search.Field.Text.PhoneticMatcher.English).sortable().build();
    public static final Field ABV = Field.numeric("abv").sortable().build();
    public static final Field OUNCES = Field.numeric("ounces").sortable().build();

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

    public static void createIndex(RedisModulesClient client) throws Throwable {
        StatefulRedisModulesConnection<String, String> connection = client.connect();
        connection.sync().create(INDEX, CreateOptions.<String, String>builder().prefix(PREFIX).build(), ID, NAME, STYLE, ABV, OUNCES);
        JsonItemReader<Map<String, Object>> reader = mapReader();
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client).operation(Hset.<String, Map<String, String>>key(m -> PREFIX + m.get(ID.getName())).map(m -> m).build()).build();
        JobFactory jobFactory = JobFactory.inMemory();
        jobFactory.run("create-beers", reader, new MapFlattener(), writer);
    }


}