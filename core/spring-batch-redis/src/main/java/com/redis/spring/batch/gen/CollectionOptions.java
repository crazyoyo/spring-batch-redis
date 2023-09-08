package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.LongRange;

public class CollectionOptions {

    public static final LongRange DEFAULT_MEMBER_RANGE = LongRange.between(1, 100);

    public static final LongRange DEFAULT_MEMBER_COUNT = LongRange.is(100);

    private LongRange memberRange = DEFAULT_MEMBER_RANGE;

    private LongRange memberCount = DEFAULT_MEMBER_COUNT;

    public LongRange getMemberRange() {
        return memberRange;
    }

    public void setMemberRange(LongRange range) {
        this.memberRange = range;
    }

    public LongRange getMemberCount() {
        return memberCount;
    }

    public void setMemberCount(LongRange count) {
        this.memberCount = count;
    }

}
