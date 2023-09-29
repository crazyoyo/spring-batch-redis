package com.redis.spring.batch.test;

public class Geo {

    private String member;

    private double longitude;

    private double latitude;

    public Geo(String member, double longitude, double latitude) {
        this.member = member;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public String getMember() {
        return member;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

}
