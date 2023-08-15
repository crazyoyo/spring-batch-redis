package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.util.Helper;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;

public class ScanSizeEstimator implements LongSupplier {

    public static final long UNKNOWN_SIZE = -1;

    public static final long DEFAULT_SAMPLE_SIZE = 100;

    private static final String FILENAME = "randomkeytype.lua";

    public static final int DEFAULT_SAMPLES = 100;

    private final AbstractRedisClient client;

    private String scanMatch;

    private String scanType;

    private int samples = DEFAULT_SAMPLES;

    public ScanSizeEstimator(AbstractRedisClient client) {
        this.client = client;
    }

    public String getScanMatch() {
        return scanMatch;
    }

    public void setScanMatch(String match) {
        this.scanMatch = match;
    }

    public int getSamples() {
        return samples;
    }

    public void setSamples(int samples) {
        this.samples = samples;
    }

    public void setScanType(String type) {
        this.scanType = type;
    }

    public String getScanType() {
        return scanType;
    }

    /**
     * Estimates the number of keys that match the given pattern and type.
     * 
     * @return Estimated number of keys matching the given pattern and type. Returns null if database is empty or any error
     *         occurs
     * @throws TimeoutException
     * @throws ExecutionException
     */
    @SuppressWarnings("unchecked")
    @Override
    public long getAsLong() {
        StatefulConnection<String, String> connection = RedisModulesUtils.connection(client);
        BaseRedisCommands<String, String> sync = Helper.sync(connection);
        Long dbsize = ((RedisServerCommands<String, String>) sync).dbsize();
        if (dbsize == null) {
            return UNKNOWN_SIZE;
        }
        if (scanMatch == null && scanType == null) {
            return dbsize;
        }
        String digest = Helper.loadScript(client, FILENAME);
        RedisScriptingAsyncCommands<String, String> commands = Helper.async(connection);
        try {
            connection.setAutoFlushCommands(false);
            List<RedisFuture<List<Object>>> futures = new ArrayList<>();
            for (int index = 0; index < samples; index++) {
                futures.add(commands.evalsha(digest, ScriptOutputType.MULTI));
            }
            connection.flushCommands();
            Predicate<String> matchPredicate = matchPredicate();
            Predicate<String> typePredicate = typePredicate();
            int total = 0;
            int matchCount = 0;
            for (RedisFuture<List<Object>> future : futures) {
                List<Object> result = future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
                if (result.size() != 2) {
                    continue;
                }
                String key = (String) result.get(0);
                String keyType = (String) result.get(1);
                total++;
                if (matchPredicate.test(key) && typePredicate.test(keyType)) {
                    matchCount++;
                }
            }
            double matchRate = total == 0 ? 0 : (double) matchCount / total;
            return Math.round(dbsize * matchRate);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Ignore and return unknown size
        } finally {
            connection.setAutoFlushCommands(true);
        }
        return UNKNOWN_SIZE;
    }

    private Predicate<String> matchPredicate() {
        if (scanMatch == null) {
            return s -> true;
        }
        MatchingEngine matchingEngine = GlobPattern.compile(scanMatch);
        return matchingEngine::matches;
    }

    private Predicate<String> typePredicate() {
        if (scanType == null) {
            return s -> true;
        }
        return scanType::equalsIgnoreCase;
    }

}
