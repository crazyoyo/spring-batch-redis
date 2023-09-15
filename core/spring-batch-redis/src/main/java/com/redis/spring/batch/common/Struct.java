package com.redis.spring.batch.common;

import java.util.Map;

public class Struct<K> extends KeyValue<K, Object> {

    private final Type type;

    public Struct(Type type, K key, Object value) {
        super(key, value);
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public enum Type {

        NONE("none"),
        HASH("hash"),
        JSON("ReJSON-RL"),
        LIST("list"),
        SET("set"),
        STREAM("stream"),
        STRING("string"),
        TIMESERIES("TSDB-TYPE"),
        ZSET("zset");

        private final String string;

        private Type(String string) {
            this.string = string;
        }

        public String getString() {
            return string;
        }

        public static Type of(String string) {
            for (Type type : Type.values()) {
                if (type.getString().equalsIgnoreCase(string)) {
                    return type;
                }
            }
            return NONE;
        }

    }

    public static <K> Struct<K> hash(K key, Map<K, ?> map) {
        return new Struct<>(Type.HASH, key, map);
    }

    public static <K> Struct<K> string(K key, Object string) {
        return new Struct<>(Type.STRING, key, string);
    }

}
