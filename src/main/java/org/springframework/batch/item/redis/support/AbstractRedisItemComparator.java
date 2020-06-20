package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractRedisItemComparator<K, TV, T extends AbstractKeyValue<K, TV>> extends AbstractItemStreamItemWriter<T> {

    private final ItemProcessor<List<? extends K>, List<T>> targetProcessor;
    private final long ttlTolerance;
    @Getter
    private final List<K> ok = new ArrayList<>();
    @Getter
    private final List<K> badValues = new ArrayList<>();
    @Getter
    private final List<K> missingKeys = new ArrayList<>();
    @Getter
    private final List<K> extraKeys = new ArrayList<>();
    @Getter
    private final List<K> badTtls = new ArrayList<>();

    protected AbstractRedisItemComparator(ItemProcessor<List<? extends K>, List<T>> targetProcessor, long ttlTolerance) {
        setName(ClassUtils.getShortName(getClass()));
        this.targetProcessor = targetProcessor;
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public void write(List<? extends T> sources) throws Exception {
        List<K> keys = sources.stream().map(AbstractKeyValue::getKey).collect(Collectors.toList());
        List<T> targets = targetProcessor.process(keys);
        if (targets == null) {
            return;
        }
        for (int index = 0; index < sources.size(); index++) {
            T source = sources.get(index);
            T target = targets.get(index);
            compare(source, target).add(source.getKey());
        }
    }

    private List<K> compare(T source, T target) {
        if (source.getValue() == null) {
            if (target.getValue() == null) {
                return ok;
            }
            return extraKeys;
        }
        if (target.getValue() == null) {
            return missingKeys;
        }
        if (equals(source.getValue(), target.getValue())) {
            if (Math.abs(source.getTtl() - target.getTtl()) > ttlTolerance) {
                return badTtls;
            }
            return ok;
        }
        return badValues;
    }

    protected abstract boolean equals(TV source, TV target);

}
