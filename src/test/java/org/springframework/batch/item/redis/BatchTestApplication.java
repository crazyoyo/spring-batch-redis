package org.springframework.batch.item.redis;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@SpringBootApplication
@EnableBatchProcessing
public class BatchTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchTestApplication.class, args);
	}

	@Bean
	LettuceConnectionFactory lettuceConnectionFactory() {
		return new LettuceConnectionFactory();
	}

	@Bean
	RedisTemplate<Object, Object> redisTemplate() {
		final RedisTemplate<Object, Object> template = new RedisTemplate<>();
		template.setConnectionFactory(lettuceConnectionFactory());
		template.setKeySerializer(new StringRedisSerializer());
		template.setHashValueSerializer(new GenericToStringSerializer<Object>(Object.class));
		template.setValueSerializer(new GenericToStringSerializer<Object>(Object.class));
		template.setDefaultSerializer(new StringRedisSerializer());
		template.setStringSerializer(new StringRedisSerializer());
		return template;
	}

}