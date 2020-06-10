package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractRedisItemComparator<K, V, TV, T extends AbstractKeyValue<K, TV>> extends AbstractItemStreamItemWriter<T> {

    private final AbstractRedisItemReader<K, V, ?, T> targetReader;
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

    protected AbstractRedisItemComparator(AbstractRedisItemReader<K, V, ?, T> targetReader, long ttlTolerance) {
        setName(ClassUtils.getShortName(getClass()));
        this.targetReader = targetReader;
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public void write(List<? extends T> sources) throws Exception {
        List<K> keys = sources.stream().map(AbstractKeyValue::getKey).collect(Collectors.toList());
        List<T> targets = targetReader.read(keys);
        for (int index = 0; index < sources.size(); index++) {
            T source = sources.get(index);
            compare(source, targets.get(index)).add(source.getKey());
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
