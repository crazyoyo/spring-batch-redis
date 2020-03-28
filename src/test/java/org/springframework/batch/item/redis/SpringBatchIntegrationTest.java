package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.redis.KeyValueRedisItemReader.KeyValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchIntegrationTest {

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private StringRedisTemplate template;

	@Test
	public void testReadHashes() throws Exception {
		int count = 100;
		template.getConnectionFactory().getConnection().flushAll();
		Map<String, String> hash = Map.of("key1", "value1", "key2", "value2");
		for (int index = 0; index < count; index++) {
			template.opsForHash().putAll("hash:" + String.format("%02d", index), hash);
		}
		List<KeyValue<String>> values = runKeyValueJob("readHzashes");
		Assertions.assertEquals(count, values.size());
		Assertions.assertEquals(values.get(0).getType(), DataType.HASH);
		Assertions.assertTrue(values.get(0).getValue() instanceof Map);
		Assertions.assertEquals(hash, values.get(0).getValue());
	}

	@Test
	public void testReadStrings() throws Exception {
		int count = 100;
		template.getConnectionFactory().getConnection().flushAll();
		for (int index = 0; index < count; index++) {
			String id = String.format("%02d", index);
			template.opsForValue().set("string:" + id, id);
		}
		List<KeyValue<String>> values = runKeyValueJob("readStrings");
		Assertions.assertEquals(count, values.size());
		Assertions.assertEquals(values.get(0).getType(), DataType.STRING);
		Assertions.assertTrue(values.get(0).getValue() instanceof String);
		Assertions.assertTrue(((String) values.get(0).getKey()).startsWith("string:"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReadStream() throws Exception {
		int count = 100;
		template.getConnectionFactory().getConnection().flushAll();
		Map<String, String> body = Map.of("key1", "value1", "key2", "value2");
		String key = "stream:1";
		for (int index = 0; index < count; index++) {
			template.opsForStream().add(key, body);
		}
		List<KeyValue<String>> values = runKeyValueJob("readStream");
		Assertions.assertEquals(1, values.size());
		Assertions.assertEquals(values.get(0).getType(), DataType.STREAM);
		Assertions.assertTrue(values.get(0).getValue() instanceof List);
		List<MapRecord<String, String, String>> messages = (List<MapRecord<String, String, String>>) values.get(0)
				.getValue();
		Assertions.assertEquals(count, messages.size());
		Map<String, String> actualBody = messages.get(0).getValue();
		Assertions.assertEquals(body, actualBody);
	}

	@Test
	public void testReadZSet() throws Exception {
		int count = 100;
		int size = 10;
		template.getConnectionFactory().getConnection().flushAll();
		for (int index = 0; index < count; index++) {
			String key = "zset:" + index;
			for (int i = 0; i < size; i++) {
				template.opsForZSet().add(key, String.valueOf(i), i);
			}
		}
		List<KeyValue<String>> values = runKeyValueJob("readZSet");
		Assertions.assertEquals(count, values.size());
		Assertions.assertEquals(values.get(0).getType(), DataType.ZSET);
		Assertions.assertTrue(values.get(0).getValue() instanceof Set);
		@SuppressWarnings("unchecked")
		Set<TypedTuple<String>> zset = (Set<TypedTuple<String>>) values.get(0).getValue();
		Assertions.assertEquals(size, zset.size());
		for (TypedTuple<String> tuple : zset) {
			Double score = tuple.getScore();
			String value = tuple.getValue();
			Assertions.assertEquals(Double.parseDouble(value), score);
		}
	}

	private List<KeyValue<String>> runKeyValueJob(String name) throws JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		List<KeyValue<String>> values = new ArrayList<>();
		KeyValueRedisItemReader<String, String> reader = KeyValueRedisItemReader.<String, String>builder()
				.redisTemplate(template).batchSize(10).saveState(false).scanOptions(ScanOptions.scanOptions().build())
				.build();
		TaskletStep redisStep = stepBuilderFactory.get(name + "Step").<KeyValue<String>, KeyValue<String>>chunk(10)
				.reader(reader).writer(l -> values.addAll(l)).build();
		Job job = jobBuilderFactory.get(name + "Job").start(redisStep).build();
		jobLauncher.run(job, new JobParameters());
		return values;
	}

}