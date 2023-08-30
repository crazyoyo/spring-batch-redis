package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.IntRange;

public class CollectionOptions {

    public static final IntRange DEFAULT_MEMBER_RANGE = IntRange.between(1, 100);

    public static final IntRange DEFAULT_MEMBER_COUNT = IntRange.is(100);

    private IntRange memberRange = DEFAULT_MEMBER_RANGE;

    private IntRange memberCount = DEFAULT_MEMBER_COUNT;

    public IntRange getMemberRange() {
        return memberRange;
    }

    public void setMemberRange(IntRange range) {
        this.memberRange = range;
    }

    public IntRange getMemberCount() {
        return memberCount;
    }

    public void setMemberCount(IntRange count) {
        this.memberCount = count;
    }

}
