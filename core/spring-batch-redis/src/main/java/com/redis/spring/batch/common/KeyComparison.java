package com.redis.spring.batch.common;

import java.util.Objects;

public class KeyComparison<T> {

    public enum Status {
        OK, // No difference
        MISSING, // Key missing in target database
        TYPE, // Type mismatch
        TTL, // TTL mismatch
        VALUE // Value mismatch
    }

    private T source;

    private T target;

    private Status status;

    public KeyComparison() {
    }

    private KeyComparison(Builder<T> builder) {
        this.source = builder.source;
        this.target = builder.target;
        this.status = builder.status;
    }

    public T getSource() {
        return source;
    }

    public void setSource(T source) {
        this.source = source;
    }

    public T getTarget() {
        return target;
    }

    public void setTarget(T target) {
        this.target = target;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(source, status, target);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        KeyComparison<?> other = (KeyComparison<?>) obj;
        return Objects.equals(source, other.source) && status == other.status && Objects.equals(target, other.target);
    }

    @Override
    public String toString() {
        return "KeyComparison [source=" + source + ", target=" + target + ", status=" + status + "]";
    }

    public static <T> Builder<T> source(T source) {
        return new Builder<>(source);
    }

    public static class Builder<T> {

        private final T source;

        private T target;

        private Status status;

        public Builder(T source) {
            this.source = source;
        }

        public Builder<T> target(T target) {
            this.target = target;
            return this;
        }

        public Builder<T> status(Status status) {
            this.status = status;
            return this;
        }

        public KeyComparison<T> build() {
            return new KeyComparison<>(this);
        }

    }

}
