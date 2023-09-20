package com.redis.spring.batch.common;

import java.util.Map;

public class Struct<K> extends KeyValue<K, Object> {

    private Type type;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
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

    public static <K> Struct<K> of(K key, Type type, Object value) {
        Struct<K> struct = new Struct<>();
        struct.setKey(key);
        struct.setType(type);
        struct.setValue(value);
        return struct;
    }

    public static <K> Struct<K> hash(K key, Map<K, ?> map) {
        return of(key, Type.HASH, map);
    }

    public static <K> Struct<K> string(K key, Object string) {
        return of(key, Type.STRING, string);
    }

}
