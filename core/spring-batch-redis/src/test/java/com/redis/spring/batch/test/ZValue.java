package com.redis.spring.batch.test;

public class ZValue {

    private String member;

    private double score;

    public ZValue(String member, double score) {
        super();
        this.member = member;
        this.score = score;
    }

    public String getMember() {
        return member;
    }

    public double getScore() {
        return score;
    }

}
