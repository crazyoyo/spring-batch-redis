package com.redis.spring.batch;

import java.util.function.ToLongFunction;

import com.redis.spring.batch.writer.AbstractOperationItemWriter;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.StructOperation;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemWriter<K, V> extends AbstractOperationItemWriter<K, V, KeyValue<K>> {

    public enum MergePolicy {
        MERGE, OVERWRITE
    }

    public enum TtlPolicy {
        PROPAGATE, DROP
    }

    public enum StreamIdPolicy {
        PROPAGATE, DROP
    }

    public static final MergePolicy DEFAULT_MERGE_POLICY = MergePolicy.OVERWRITE;

    public static final StreamIdPolicy DEFAULT_STREAM_ID_POLICY = StreamIdPolicy.PROPAGATE;

    public static final TtlPolicy DEFAULT_TTL_POLICY = TtlPolicy.PROPAGATE;

    private TtlPolicy ttlPolicy = DEFAULT_TTL_POLICY;

    private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;

    private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

    private ValueType valueType = ValueType.DUMP;

    public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public TtlPolicy getTtlPolicy() {
        return ttlPolicy;
    }

    public void setTtlPolicy(TtlPolicy policy) {
        this.ttlPolicy = policy;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(MergePolicy policy) {
        this.mergePolicy = policy;
    }

    public StreamIdPolicy getStreamIdPolicy() {
        return streamIdPolicy;
    }

    public void setStreamIdPolicy(StreamIdPolicy policy) {
        this.streamIdPolicy = policy;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    @Override
    protected Operation<K, V, KeyValue<K>> operation() {
        if (valueType == ValueType.DUMP) {
            return new RestoreReplace<>(KeyValue::getKey, v -> (byte[]) v.getValue(), keyValueTtl());
        }
        StructOperation<K, V> operation = new StructOperation<>();
        operation.setMergePolicy(mergePolicy);
        operation.setStreamIdPolicy(streamIdPolicy);
        operation.setTtlPolicy(ttlPolicy);
        return operation;
    }

    private ToLongFunction<KeyValue<K>> keyValueTtl() {
        if (ttlPolicy == TtlPolicy.PROPAGATE) {
            return KeyValue::getTtl;
        }
        return kv -> 0;
    }

}
