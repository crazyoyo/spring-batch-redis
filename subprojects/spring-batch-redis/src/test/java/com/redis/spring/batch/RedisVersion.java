package com.redis.spring.batch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

public class RedisVersion {

    private static Pattern redisVersionPattern = Pattern.compile("redis_version:(\\d)\\.(\\d).*");

    public static RedisVersion UNKNOWN = new RedisVersion(0, 0);

    private final int major;

    private final int minor;

    public RedisVersion(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public static RedisVersion of(StatefulRedisModulesConnection<String, String> connection) {
        String info = connection.sync().info("SERVER");
        Matcher matcher = redisVersionPattern.matcher(info);
        if (matcher.find()) {
            int major = Integer.parseInt(matcher.group(1));
            int minor = Integer.parseInt(matcher.group(2));
            return new RedisVersion(major, minor);
        }
        return UNKNOWN;
    }

}
