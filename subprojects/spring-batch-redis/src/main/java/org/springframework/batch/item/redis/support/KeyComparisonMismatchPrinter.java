package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.extern.slf4j.Slf4j;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.ListCompareAlgorithm;

import java.util.Collection;

@Slf4j
public class KeyComparisonMismatchPrinter implements KeyComparisonItemWriter.KeyComparisonResultHandler {

    private Javers javers = JaversBuilder.javers().withListCompareAlgorithm(ListCompareAlgorithm.LEVENSHTEIN_DISTANCE).build();

    @Override
    public void accept(DataStructure source, DataStructure target, KeyComparisonItemWriter.Status status) {
        switch (status) {
            case SOURCE:
                log.warn("Missing key '{}'", source.getKey());
                break;
            case TARGET:
                log.warn("Extraneous key '{}'", target.getKey());
                break;
            case TTL:
                log.warn("TTL mismatch for key '{}': {} <> {}", source.getKey(), source.getAbsoluteTTL(), target.getAbsoluteTTL());
                break;
            case TYPE:
                log.warn("Type mismatch for key '{}': {} <> {}", source.getKey(), source.getType(), target.getType());
                break;
            case VALUE:
                switch (source.getType()) {
                    case DataStructure.SET:
                    case DataStructure.LIST:
                        diffCollections(source, target, String.class);
                        break;
                    case DataStructure.ZSET:
                        diffCollections(source, target, ScoredValue.class);
                        break;
                    case DataStructure.STREAM:
                        diffCollections(source, target, StreamMessage.class);
                        break;
                    default:
                        log.warn("Value mismatch for {} '{}': {}", source.getType(), source.getKey(), javers.compare(source, target).prettyPrint());
                        break;
                }
                break;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void diffCollections(DataStructure source, DataStructure target, Class<T> itemClass) {
        Collection<T> sourceValue = (Collection<T>) source.getValue();
        Collection<T> targetValue = (Collection<T>) target.getValue();
        if (Math.abs(sourceValue.size() - targetValue.size()) > 5) {
            log.warn("Size mismatch for {} '{}': {} <> {}", source.getType(), source.getKey(), sourceValue.size(), targetValue.size());
        } else {
            log.warn("Value mismatch for {} '{}'", source.getType(), source.getKey());
            log.info("{}", javers.compareCollections(sourceValue, targetValue, itemClass).prettyPrint());
        }
    }

}