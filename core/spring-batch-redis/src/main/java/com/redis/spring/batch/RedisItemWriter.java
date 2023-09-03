package com.redis.spring.batch;

import java.util.function.ToLongFunction;

import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructOperation;
import com.redis.spring.batch.writer.operation.Restore;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemWriter<K, V> extends OperationItemWriter<K, V, KeyValue<K>> {

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

    public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

    private TtlPolicy ttlPolicy = DEFAULT_TTL_POLICY;

    private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;

    private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

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
    protected void doOpen() {
        setOperation(operation());
        super.doOpen();
    }

    private Operation<K, V, KeyValue<K>> operation() {
        if (valueType == ValueType.DUMP) {
            Restore<K, V, KeyValue<K>> operation = new Restore<>();
            operation.setKey(KeyValue::getKey);
            operation.setBytes(v -> (byte[]) v.getValue());
            operation.setTtl(keyValueTtl());
            operation.setReplace(true);
            return operation;
        }
        StructOperation<K, V> operation = new StructOperation<>();
        operation.mergePolicy(mergePolicy);
        operation.streamIdPolicy(streamIdPolicy);
        operation.ttlPolicy(ttlPolicy);
        return operation;
    }

    private ToLongFunction<KeyValue<K>> keyValueTtl() {
        if (ttlPolicy == TtlPolicy.PROPAGATE) {
            return KeyValue::getTtl;
        }
        return kv -> 0;
    }

}
