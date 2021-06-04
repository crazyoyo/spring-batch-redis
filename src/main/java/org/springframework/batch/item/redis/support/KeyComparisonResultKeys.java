package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyComparisonResultKeys implements KeyComparisonItemWriter.KeyComparisonResultHandler {

    private final Map<KeyComparisonItemWriter.Result, List> keyLists = Arrays.stream(KeyComparisonItemWriter.Result.values()).collect(Collectors.toMap(Function.identity(), r -> new ArrayList<>()));

    @Override
    public void accept(DataStructure source, DataStructure target, KeyComparisonItemWriter.Result result) {
        keyLists.get(result).add(source.getKey());
    }

    public List get(KeyComparisonItemWriter.Result result) {
        return keyLists.get(result);
    }

    public boolean isOK() {
        if (get(KeyComparisonItemWriter.Result.OK).isEmpty()) {
            return false;
        }
        for (KeyComparisonItemWriter.Result mismatch : KeyComparisonItemWriter.MISMATCHES) {
            if (!get(mismatch).isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
