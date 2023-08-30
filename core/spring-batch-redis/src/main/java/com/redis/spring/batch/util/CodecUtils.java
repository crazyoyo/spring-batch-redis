package com.redis.spring.batch.util;

import java.util.function.Function;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class CodecUtils {

    private CodecUtils() {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K> Function<String, K> stringKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return key -> codec.decodeKey(StringCodec.UTF8.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K> Function<K, String> toStringKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return key -> StringCodec.UTF8.decodeKey(codec.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <V> Function<String, V> stringValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return value -> codec.decodeValue(StringCodec.UTF8.encodeValue(value));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <V> Function<V, String> toStringValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof StringCodec) {
            return (Function) Function.identity();
        }
        return value -> StringCodec.UTF8.decodeValue(codec.encodeValue(value));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K> Function<byte[], K> byteArrayKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return key -> codec.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K> Function<K, byte[]> toByteArrayKeyFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return key -> ByteArrayCodec.INSTANCE.decodeKey(codec.encodeKey(key));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <V> Function<byte[], V> byteArrayValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return value -> codec.decodeValue(ByteArrayCodec.INSTANCE.encodeValue(value));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <V> Function<V, byte[]> toByteArrayValueFunction(RedisCodec<?, V> codec) {
        if (codec instanceof ByteArrayCodec) {
            return (Function) Function.identity();
        }
        return value -> ByteArrayCodec.INSTANCE.decodeValue(codec.encodeValue(value));
    }

}
