package com.redis.spring.batch.support;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyComparisonResultKeys<K> implements KeyComparisonItemWriter.KeyComparisonResultHandler<K> {

    @Getter
    private final Map<KeyComparisonItemWriter.Status, List<K>> results = Arrays.stream(KeyComparisonItemWriter.Status.values()).collect(Collectors.toMap(Function.identity(), r -> new ArrayList<>()));

    @Override
    public void accept(DataStructure<K> source, DataStructure<K> target, KeyComparisonItemWriter.Status status) {
        results.get(status).add(source.getKey());
    }

    public List<K> get(KeyComparisonItemWriter.Status status) {
        return results.get(status);
    }

}
