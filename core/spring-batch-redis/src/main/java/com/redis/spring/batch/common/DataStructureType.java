package com.redis.spring.batch.common;

public enum DataStructureType {

    NONE("none"), HASH("hash"), JSON("ReJSON-RL"), LIST("list"), SET("set"), STREAM("stream"), STRING("string"), TIMESERIES(
            "TSDB-TYPE"), ZSET("zset");

    private final String string;

    private DataStructureType(String string) {
        this.string = string;
    }

    public String getString() {
        return string;
    }

    public static DataStructureType of(String string) {
        for (DataStructureType type : DataStructureType.values()) {
            if (type.getString().equalsIgnoreCase(string)) {
                return type;
            }
        }
        return NONE;
    }

}
