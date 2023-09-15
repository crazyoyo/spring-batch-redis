package com.redis.spring.batch.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.hrakaroo.glob.GlobPattern;
import com.redis.spring.batch.common.Range;

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

    static IntPredicate intAnd(IntPredicate... predicates) {
        return intAnd(Arrays.asList(predicates));
    }

    static IntPredicate intAnd(Collection<IntPredicate> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return intIsTrue();
        }
        return intAnd(predicates.stream());
    }

    static IntPredicate intAnd(Stream<IntPredicate> predicates) {
        return predicates.reduce(intIsTrue(), IntPredicate::and);
    }

    static IntPredicate intOr(IntPredicate... predicates) {
        return intOr(Arrays.asList(predicates));
    }

    static IntPredicate intOr(Collection<IntPredicate> predicates) {
        if (CollectionUtils.isEmpty(predicates)) {
            return intIsTrue();
        }
        return intOr(predicates.stream());
    }

    static IntPredicate intOr(Stream<IntPredicate> predicates) {
        return predicates.reduce(intIsFalse(), IntPredicate::or);
    }

    static <T> Predicate<T> intMap(ToIntFunction<T> function, IntPredicate predicate) {
        return k -> predicate.test(function.applyAsInt(k));
    }

    static <S, T> Predicate<S> map(Function<S, T> function, Predicate<T> predicate) {
        return s -> predicate.test(function.apply(s));
    }

    static Predicate<String> slots(Collection<Range> ranges) {
        return slots(StringCodec.UTF8, ranges);
    }

    static <K> Predicate<K> slots(RedisCodec<K, ?> codec, Collection<Range> ranges) {
        if (CollectionUtils.isEmpty(ranges)) {
            return isTrue();
        }
        return slots(codec, ranges.stream());
    }

    static <K> Predicate<K> slots(RedisCodec<K, ?> codec, Range... ranges) {
        return slots(codec, Arrays.asList(ranges));
    }

    static Predicate<String> slots(Range... ranges) {
        return slots(StringCodec.UTF8, ranges);
    }

    static <K> Predicate<K> slots(RedisCodec<K, ?> codec, Stream<Range> ranges) {
        ToIntFunction<K> slot = k -> SlotHash.getSlot(codec.encodeKey(k));
        IntPredicate predicate = intAnd(ranges.map(Range::asPredicate).toArray(IntPredicate[]::new));
        return intMap(slot, predicate);
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

    static IntPredicate intIsTrue() {
        return intIs(true);
    }

    /**
     * A {@link Predicate} that yields always {@code false}.
     *
     * @return a {@link Predicate} that yields always {@code false}.
     */
    static <T> Predicate<T> isFalse() {
        return is(false);
    }

    static IntPredicate intIsFalse() {
        return intIs(false);
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

    static IntPredicate intNegate(IntPredicate predicate) {

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

    static IntPredicate intIs(boolean value) {
        return t -> value;
    }

}
