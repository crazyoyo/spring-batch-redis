package com.redis.spring.batch.common;

public class KeyComparison {

    public enum Status {
        OK, // No difference
        MISSING, // Key missing in target database
        TYPE, // Type mismatch
        TTL, // TTL mismatch
        VALUE // Value mismatch
    }

    private KeyValue<String> source;

    private KeyValue<String> target;

    private Status status;

    public KeyValue<String> getSource() {
        return source;
    }

    public void setSource(KeyValue<String> source) {
        this.source = source;
    }

    public KeyValue<String> getTarget() {
        return target;
    }

    public void setTarget(KeyValue<String> target) {
        this.target = target;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

}
