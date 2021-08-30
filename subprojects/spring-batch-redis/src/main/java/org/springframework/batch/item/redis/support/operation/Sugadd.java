package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.SugaddOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Sugadd<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Double> score;
    private final Converter<T, String> payload;
    private final boolean increment;
    private final Converter<T, String> string;

    public Sugadd(Converter<T, Object> key, Converter<T, String> string, Converter<T, Double> score, Converter<T, String> payload, boolean increment) {
        super(key, t -> false);
        Assert.notNull(string, "A string converter is required");
        Assert.notNull(score, "A score converter is required");
        this.string = string;
        this.score = score;
        this.payload = payload;
        this.increment = increment;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Double score = this.score.convert(item);
        if (score == null) {
            return null;
        }
        V string = (V) this.string.convert(item);
        if (payload == null && !increment) {
            return ((RedisModulesAsyncCommands<K, V>) commands).sugadd(key, string, score);
        }
        SugaddOptions.SugaddOptionsBuilder<K, V> options = SugaddOptions.builder();
        options.increment(increment);
        if (payload != null) {
            options.payload((V) payload.convert(item));
        }
        return ((RedisModulesAsyncCommands<K, V>) commands).sugadd(key, string, score, options.build());
    }

}
