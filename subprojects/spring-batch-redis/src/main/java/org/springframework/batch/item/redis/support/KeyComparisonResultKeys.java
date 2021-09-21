package org.springframework.batch.item.redis.support;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyComparisonResultKeys implements KeyComparisonItemWriter.KeyComparisonResultHandler {

    @Getter
    private final Map<KeyComparisonItemWriter.Status, List<String>> results = Arrays.stream(KeyComparisonItemWriter.Status.values()).collect(Collectors.toMap(Function.identity(), r -> new ArrayList<>()));

    @Override
    public void accept(DataStructure source, DataStructure target, KeyComparisonItemWriter.Status status) {
        results.get(status).add(source.getKey());
    }

    public List<String> get(KeyComparisonItemWriter.Status status) {
        return results.get(status);
    }

}
