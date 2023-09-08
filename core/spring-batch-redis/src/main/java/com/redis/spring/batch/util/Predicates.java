package com.redis.spring.batch.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.hrakaroo.glob.GlobPattern;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public interface Predicates {

    @SuppressWarnings("unchecked")
    static <T> Predicate<T> and(Predicate<T>... predicates) {
        return and(Arrays.asList(predicates));
    }

    static <T> Predicate<T> and(Collection<Predicate<T>> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return isTrue();
        }
        if (predicates.size() == 1) {
            return predicates.iterator().next();
        }
        return and(predicates.stream());
    }

    static <T> Predicate<T> and(Stream<Predicate<T>> predicates) {
        return predicates.reduce(isTrue(), Predicate::and);
    }

    @SuppressWarnings("unchecked")
    static <T> Predicate<T> or(Predicate<T>... predicates) {
        return or(Arrays.asList(predicates));
    }

    static <T> Predicate<T> or(Collection<Predicate<T>> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return isTrue();
        }
        if (predicates.size() == 1) {
            return predicates.iterator().next();
        }
        return or(predicates.stream());
    }

    static <T> Predicate<T> or(Stream<Predicate<T>> predicates) {
        return predicates.reduce(isFalse(), Predicate::or);
    }

    static LongPredicate longAnd(LongPredicate... predicates) {
        return longAnd(Arrays.asList(predicates));
    }

    static LongPredicate longAnd(Collection<LongPredicate> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return longIsTrue();
        }
        return longAnd(predicates.stream());
    }

    static LongPredicate longAnd(Stream<LongPredicate> predicates) {
        return predicates.reduce(longIsTrue(), LongPredicate::and);
    }

    static LongPredicate longOr(LongPredicate... predicates) {
        return longOr(Arrays.asList(predicates));
    }

    static LongPredicate longOr(Collection<LongPredicate> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return longIsTrue();
        }
        return longOr(predicates.stream());
    }

    static LongPredicate longOr(Stream<LongPredicate> predicates) {
        return predicates.reduce(longIsFalse(), LongPredicate::or);
    }

    static <T> Predicate<T> longMap(ToLongFunction<T> function, LongPredicate predicate) {
        return k -> predicate.test(function.applyAsLong(k));
    }

    static <S, T> Predicate<S> map(Function<S, T> function, Predicate<T> predicate) {
        return s -> predicate.test(function.apply(s));
    }

    static Predicate<String> slots(Collection<LongRange> ranges) {
        return slots(StringCodec.UTF8, ranges);
    }

    static <K> Predicate<K> slots(RedisCodec<K, ?> codec, Collection<LongRange> ranges) {
        if (CollectionUtils.isEmpty(ranges)) {
            return isTrue();
        }
        return slots(codec, ranges.stream());
    }

    static <K> Predicate<K> slots(RedisCodec<K, ?> codec, LongRange... ranges) {
        return slots(codec, Arrays.asList(ranges));
    }

    static Predicate<String> slots(LongRange... ranges) {
        return slots(StringCodec.UTF8, ranges);
    }

    static <K> Predicate<K> slots(RedisCodec<K, ?> codec, Stream<LongRange> ranges) {
        ToLongFunction<K> slot = k -> SlotHash.getSlot(codec.encodeKey(k));
        LongPredicate predicate = longAnd(ranges.map(LongRange::asPredicate).toArray(LongPredicate[]::new));
        return longMap(slot, predicate);
    }

    static Predicate<String> regex(String regex) {
        return Pattern.compile(regex).asPredicate();
    }

    /**
     * A {@link Predicate} that yields always {@code true}.
     *
     * @return a {@link Predicate} that yields always {@code true}.
     */
    static <T> Predicate<T> isTrue() {
        return is(true);
    }

    static LongPredicate longIsTrue() {
        return longIs(true);
    }

    /**
     * A {@link Predicate} that yields always {@code false}.
     *
     * @return a {@link Predicate} that yields always {@code false}.
     */
    static <T> Predicate<T> isFalse() {
        return is(false);
    }

    static LongPredicate longIsFalse() {
        return longIs(false);
    }

    /**
     * Returns a {@link Predicate} that represents the logical negation of {@code predicate}.
     *
     * @return a {@link Predicate} that represents the logical negation of {@code predicate}.
     */
    static <T> Predicate<T> negate(Predicate<T> predicate) {

        Assert.notNull(predicate, "Predicate must not be null");
        return predicate.negate();
    }

    static LongPredicate longNegate(LongPredicate predicate) {

        Assert.notNull(predicate, "Predicate must not be null");
        return predicate.negate();
    }

    static Predicate<String> glob(String match) {
        if (!StringUtils.hasLength(match)) {
            return isTrue();
        }
        return GlobPattern.compile(match)::matches;
    }

    static <T> Predicate<T> is(boolean value) {
        return t -> value;
    }

    static LongPredicate longIs(boolean value) {
        return t -> value;
    }

}
