package org.springframework.batch.item.redis.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.JacksonJsonNodeReader;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class Beers {

	public final static String FIELD_ABV = "abv";

	public final static String FIELD_ID = "id";

	public final static String FIELD_NAME = "name";

	public final static String FIELD_STYLE = "style";

	public final static String FIELD_OUNCES = "ounces";

	private static final String BEERS_CSV = "beers.csv";

	private static final String BEERS_JSON = "beers.json";

	public static List<Map<String, String>> loadFromCsv() throws IOException {
		CsvSchema schema = CsvSchema.builder().setUseHeader(true).setNullValue("").build();
		InputStream inputStream = Beers.class.getClassLoader().getResourceAsStream(BEERS_CSV);
		return new CsvMapper().readerFor(Map.class).with(schema).<Map<String, String>>readValues(inputStream).readAll();
	}

	public static JsonItemReader<JsonNode> jsonReader() {
		JsonItemReaderBuilder<JsonNode> jsonReaderBuilder = new JsonItemReaderBuilder<>();
		jsonReaderBuilder.name("beer-json-reader");
		jsonReaderBuilder.resource(new ClassPathResource("beers.json"));
		JacksonJsonNodeReader nodeReader = new JacksonJsonNodeReader(new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false));
		jsonReaderBuilder.jsonObjectReader(nodeReader);
		return jsonReaderBuilder.build();
	}

}
