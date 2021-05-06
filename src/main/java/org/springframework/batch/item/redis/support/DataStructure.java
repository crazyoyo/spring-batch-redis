package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class DataStructure<K> extends KeyValue<K, Object> {

    public final static String STRING = "string";
    public final static String LIST = "list";
    public final static String SET = "set";
    public final static String ZSET = "zset";
    public final static String HASH = "hash";
    public final static String STREAM = "stream";
    public static final String NONE = "none";

    private String type;

    public DataStructure(K key) {
        super(key);
    }

    public DataStructure(K key, long absoluteTTL, String type) {
        super(key, absoluteTTL);
        this.type = type;
    }


    public DataStructure(K key, String type) {
        super(key);
        this.type = type;
    }

}
