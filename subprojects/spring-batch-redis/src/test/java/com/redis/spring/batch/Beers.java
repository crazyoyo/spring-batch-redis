package com.redis.spring.batch;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.JsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.redis.lettucemod.api.search.CreateOptions;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.support.JacksonJsonNodeReader;
import com.redis.spring.batch.support.convert.MapFlattener;
import com.redis.spring.batch.support.operation.Hset;

import io.lettuce.core.AbstractRedisClient;

public class Beers {

	public static final int CHUNK_SIZE = 50;

	public static final String FILE = "beers.json";
	public static final String INDEX = "beers";
	public static final String PREFIX = "beer:";

	public static final Field ID = Field.tag("id").sortable().build();
	public static final Field BREWERY_ID = Field.tag("brewery_id").sortable().build();
	public static final Field NAME = Field.text("name").sortable().build();
	public static final Field ABV = Field.numeric("abv").sortable().build();
	public static final Field IBU = Field.numeric("ibu").sortable().build();
	public static final Field DESCRIPT = Field.text("descript").sortable().build();
	public static final Field STYLE_NAME = Field.text("style_name").matcher(Field.Text.PhoneticMatcher.English)
			.sortable().build();
	public static final Field CAT_NAME = Field.text("cat_name").matcher(Field.Text.PhoneticMatcher.English).sortable()
			.build();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static JsonItemReader<Map<String, Object>> mapReader() {
		return (JsonItemReader) reader("map", FILE, new JacksonJsonObjectReader<>(Map.class));
	}

	public static JsonItemReader<JsonNode> jsonNodeReader() {
		return reader("json-node", FILE, new JacksonJsonNodeReader());
	}

	public static <T> JsonItemReader<T> reader(String name, String path, JsonObjectReader<T> objectReader) {
		return new JsonItemReaderBuilder<T>().name(name + "-reader").resource(classPathResource(path))
				.jsonObjectReader(objectReader).build();
	}

	public static Resource classPathResource(String path) {
		return new ClassPathResource(path, Beers.class.getClassLoader());
	}

	public static void createIndex(RedisModulesCommands<String, String> commands) {
		commands.create(INDEX, CreateOptions.<String, String>builder().prefix(PREFIX).build(), ID, NAME, STYLE_NAME,
				CAT_NAME, BREWERY_ID, DESCRIPT, ABV, IBU);
	}

	public static void populateIndex(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			AbstractRedisClient client) throws Exception {
		JsonItemReader<Map<String, Object>> reader = mapReader();
		RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
				.operation(Hset.<Map<String, String>>key(m -> PREFIX + m.get(ID.getName())).map(m -> m).build())
				.client(client).build();
		String name = "populate-beer-index";
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		TaskletStep step = stepBuilderFactory.get(name).<Map<String, Object>, Map<String, String>>chunk(CHUNK_SIZE)
				.reader(reader).processor(new MapFlattener()).writer(writer).build();
		Job job = new JobBuilderFactory(jobRepository).get(name).start(step).build();
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
		jobLauncher.run(job, new JobParameters());
	}

}