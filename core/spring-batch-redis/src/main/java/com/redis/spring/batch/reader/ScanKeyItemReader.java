package com.redis.spring.batch.reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

    private final Log log = LogFactory.getLog(getClass());

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private ReadFrom readFrom;

    private long limit;

    private String match;

    private String type;

    private StatefulRedisModulesConnection<K, V> connection;

    private ScanIterator<K> iterator;

    public ScanKeyItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.codec = codec;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setMatch(String match) {
        this.match = match;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (iterator == null) {
            log.debug("Opening");
            connection = ConnectionUtils.connection(client, codec, readFrom);
            iterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
        }
    }

    public boolean isOpen() {
        return iterator != null;
    }

    private KeyScanArgs scanArgs() {
        KeyScanArgs args = new KeyScanArgs();
        if (limit > 0) {
            args.limit(limit);
        }
        if (match != null) {
            args.match(match);
        }
        if (type != null) {
            args.type(type);
        }
        return args;
    }

    @Override
    public K read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public synchronized void close() {
        if (iterator != null) {
            log.debug("Closing");
            connection.close();
            iterator = null;
        }
        super.close();
    }

}
