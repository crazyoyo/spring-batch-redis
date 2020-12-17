package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface KeyMaker<T> extends Converter<T, String> {

    String DEFAULT_SEPARATOR = ":";

    String EMPTY_STRING = "";

    static <T> KeyMakerBuilder<T> builder() {
        return new KeyMakerBuilder<>();
    }

    class KeyMakerBuilder<T> {

        private String separator = DEFAULT_SEPARATOR;
        private String prefix = EMPTY_STRING;
        private List<Converter<T, String>> converters = new ArrayList<>();

        public KeyMakerBuilder<T> separator(String separator) {
            Assert.notNull(separator, "Separator cannot be null.");
            this.separator = separator;
            return this;
        }

        public KeyMakerBuilder<T> prefix(String prefix) {
            Assert.notNull(prefix, "Prefix cannot be null.");
            this.prefix = prefix;
            return this;
        }

        public KeyMakerBuilder<T> converters(Converter<T, String>... converters) {
            Assert.notNull(converters, "Key converters cannot be null.");
            this.converters = Arrays.asList(converters);
            return this;
        }

        public KeyMaker<T> build() {
            if (converters.isEmpty()) {
                return s -> prefix;
            }
            String keyspace = prefix.isEmpty() ? EMPTY_STRING : prefix + separator;
            if (converters.size() == 1) {
                Converter<T, String> converter = converters.get(0);
                return s -> keyspace + converter.convert(s);
            }
            return s -> keyspace + convert(s);
        }

        private String convert(T source) {
            List<String> elements = new ArrayList<>();
            converters.forEach(c -> elements.add(c.convert(source)));
            return String.join(separator, elements.toArray(new String[0]));
        }

    }

}
