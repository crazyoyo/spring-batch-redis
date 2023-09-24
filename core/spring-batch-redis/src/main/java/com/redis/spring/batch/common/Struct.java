package com.redis.spring.batch.common;

public class Struct<K> extends KeyValue<K, Object> {

    private DataStructureType type;

    public Struct() {
    }

    private Struct(Builder<K> builder) {
        super(builder);
        this.type = builder.type;
    }

    public DataStructureType getType() {
        return type;
    }

    public void setType(DataStructureType type) {
        this.type = type;
    }

    public boolean exists() {
        return super.exists() && type != DataStructureType.NONE;
    }

    public static <K> Builder<K> key(K key) {
        return new Builder<>(key);
    }

    public static class Builder<K> extends KeyValue.Builder<K, Object, Builder<K>> {

        private DataStructureType type;

        public Builder(K key) {
            super(key);
        }

        public Builder<K> type(DataStructureType type) {
            this.type = type;
            return this;
        }

        public Struct<K> build() {
            return new Struct<>(this);
        }

    }

}
