package org.springframework.batch.item.redis.test;

import java.util.Map;

import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.JsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.JacksonJsonNodeReader;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.batch.item.redis.support.convert.MapFlattener;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.redis.lettucemod.api.search.CreateOptions;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.sync.RedisModulesCommands;

import io.lettuce.core.AbstractRedisClient;

public class Beers {

    public static final String FILE = "beers.json";
    public static final String INDEX = "beers";
    public static final String PREFIX = "beer:";

    public static final Field ID = Field.tag("id").sortable().build();
    public static final Field BREWERY_ID = Field.tag("brewery_id").sortable().build();
    public static final Field NAME = Field.text("name").sortable().build();
    public static final Field ABV = Field.numeric("abv").sortable().build();
    public static final Field IBU = Field.numeric("ibu").sortable().build();
    public static final Field DESCRIPT = Field.text("descript").sortable().build();
    public static final Field STYLE_NAME = Field.text("style_name").matcher(Field.Text.PhoneticMatcher.English).sortable().build();
    public static final Field CAT_NAME = Field.text("cat_name").matcher(Field.Text.PhoneticMatcher.English).sortable().build();

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public static JsonItemReader<Map<String, Object>> mapReader() {
        return (JsonItemReader) reader("map", FILE, new JacksonJsonObjectReader<>(Map.class));
    }

    public static JsonItemReader<JsonNode> jsonNodeReader() {
        return reader("json-node", FILE, new JacksonJsonNodeReader());
    }

    public static <T> JsonItemReader<T> reader(String name, String path, JsonObjectReader<T> objectReader) {
        return new JsonItemReaderBuilder<T>().name(name + "-reader").resource(classPathResource(path)).jsonObjectReader(objectReader).build();
    }

    public static Resource classPathResource(String path) {
        return new ClassPathResource(path, Beers.class.getClassLoader());
    }

    public static void createIndex(RedisModulesCommands<String,String> commands) {
        commands.create(INDEX, CreateOptions.<String, String>builder().prefix(PREFIX).build(), ID, NAME, STYLE_NAME, CAT_NAME, BREWERY_ID, DESCRIPT, ABV, IBU);
    }

    public static void populateIndex(AbstractRedisClient client) throws Throwable {
        JsonItemReader<Map<String, Object>> reader = mapReader();
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client).operation(Hset.<String, Map<String, String>>key(m -> PREFIX + m.get(ID.getName())).map(m -> m).build()).build();
        JobFactory jobFactory = JobFactory.inMemory();
        jobFactory.run("create-beers", reader, new MapFlattener(), writer);
    }


}