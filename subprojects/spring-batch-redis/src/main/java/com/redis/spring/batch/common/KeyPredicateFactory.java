package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyPredicateFactory {

    private List<String> includes = new ArrayList<>();

    private List<String> excludes = new ArrayList<>();

    private List<IntRange> slotRanges = new ArrayList<>();

    public KeyPredicateFactory include(String... regexes) {
        return include(Arrays.asList(regexes));
    }

    public KeyPredicateFactory include(Collection<String> regexes) {
        includes.addAll(regexes);
        return this;
    }

    public KeyPredicateFactory exclude(String... regexes) {
        return exclude(Arrays.asList(regexes));
    }

    public KeyPredicateFactory exclude(Collection<String> regexes) {
        excludes.addAll(regexes);
        return this;
    }

    public KeyPredicateFactory slotRange(int min, int max) {
        return slotRange(IntRange.between(min, max));
    }

    public KeyPredicateFactory slotRange(IntRange... ranges) {
        return slotRange(Arrays.asList(ranges));
    }

    public KeyPredicateFactory slotRange(Collection<IntRange> ranges) {
        slotRanges.addAll(ranges);
        return this;
    }

    public <K> Predicate<K> build(RedisCodec<K, ?> codec) {
        List<Predicate<K>> predicates = new ArrayList<>();
        if (!slotRanges.isEmpty()) {
            predicates.add(slotRangePredicate(codec));
        }
        if (!includes.isEmpty() || !excludes.isEmpty()) {
            predicates.add(regexPredicate(codec));
        }
        if (predicates.isEmpty()) {
            return k -> true;
        }
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        return predicates.stream().reduce(x -> true, Predicate::and);
    }

    private <K> Predicate<K> regexPredicate(RedisCodec<K, ?> codec) {
        if (includes.isEmpty()) {
            return regexPredicate(codec, excludes).negate();
        }
        return regexPredicate(codec, includes);
    }

    private <K> Predicate<K> slotRangePredicate(RedisCodec<K, ?> codec) {
        ToIntFunction<K> slot = slotFunction(codec);
        IntPredicate rangePredicate = slotRanges.stream().map(IntRange::asPredicate).reduce(x -> true, IntPredicate::and);
        return k -> rangePredicate.test(slot.applyAsInt(k));
    }

    private static <K> Predicate<K> regexPredicate(RedisCodec<K, ?> codec, Collection<String> regexes) {
        Function<K, String> string = Utils.toStringKeyFunction(codec);
        Predicate<String> predicate = regexes.stream().map(Pattern::compile).map(KeyPredicateFactory::matchPredicate)
                .reduce(x -> false, Predicate::or);
        return k -> predicate.test(string.apply(k));
    }

    private static Predicate<String> matchPredicate(Pattern pattern) {
        return s -> pattern.matcher(s).matches();
    }

    private static <K> ToIntFunction<K> slotFunction(RedisCodec<K, ?> codec) {
        if (codec instanceof ByteArrayCodec) {
            return k -> SlotHash.getSlot((byte[]) k);
        }
        if (codec instanceof StringCodec) {
            return k -> SlotHash.getSlot((String) k);
        }
        return k -> SlotHash.getSlot(codec.encodeKey(k));
    }

    public static KeyPredicateFactory create() {
        return new KeyPredicateFactory();
    }

    public Predicate<String> build() {
        return build(StringCodec.UTF8);
    }

}
