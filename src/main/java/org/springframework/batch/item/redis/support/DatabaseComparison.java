package org.springframework.batch.item.redis.support;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class DatabaseComparison<K> {

    private final Map<KeyComparison.Result, List<K>> results;

    public DatabaseComparison(Map<KeyComparison.Result, List<K>> results) {
        this.results = results;
    }

    public boolean isIdentical() {
        for (KeyComparison.Result result : EnumSet.complementOf(EnumSet.of(KeyComparison.Result.OK))) {
            if (!results.get(result).isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
