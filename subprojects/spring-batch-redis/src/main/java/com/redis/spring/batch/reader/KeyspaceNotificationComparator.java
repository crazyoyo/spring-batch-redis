package com.redis.spring.batch.reader;

import static com.redis.spring.batch.reader.KeyEventType.APPEND;
import static com.redis.spring.batch.reader.KeyEventType.COPY_TO;
import static com.redis.spring.batch.reader.KeyEventType.DEL;
import static com.redis.spring.batch.reader.KeyEventType.EVICTED;
import static com.redis.spring.batch.reader.KeyEventType.EXPIRE;
import static com.redis.spring.batch.reader.KeyEventType.EXPIRED;
import static com.redis.spring.batch.reader.KeyEventType.HDEL;
import static com.redis.spring.batch.reader.KeyEventType.HINCRBY;
import static com.redis.spring.batch.reader.KeyEventType.HINCRBYFLOAT;
import static com.redis.spring.batch.reader.KeyEventType.HSET;
import static com.redis.spring.batch.reader.KeyEventType.INCRBY;
import static com.redis.spring.batch.reader.KeyEventType.INCRBYFLOAT;
import static com.redis.spring.batch.reader.KeyEventType.JSON_SET;
import static com.redis.spring.batch.reader.KeyEventType.LINSERT;
import static com.redis.spring.batch.reader.KeyEventType.LPOP;
import static com.redis.spring.batch.reader.KeyEventType.LPUSH;
import static com.redis.spring.batch.reader.KeyEventType.LREM;
import static com.redis.spring.batch.reader.KeyEventType.LSET;
import static com.redis.spring.batch.reader.KeyEventType.LTRIM;
import static com.redis.spring.batch.reader.KeyEventType.MOVE_FROM;
import static com.redis.spring.batch.reader.KeyEventType.MOVE_TO;
import static com.redis.spring.batch.reader.KeyEventType.NEW_KEY;
import static com.redis.spring.batch.reader.KeyEventType.PERSIST;
import static com.redis.spring.batch.reader.KeyEventType.RENAME_FROM;
import static com.redis.spring.batch.reader.KeyEventType.RENAME_TO;
import static com.redis.spring.batch.reader.KeyEventType.RESTORE;
import static com.redis.spring.batch.reader.KeyEventType.RPOP;
import static com.redis.spring.batch.reader.KeyEventType.RPUSH;
import static com.redis.spring.batch.reader.KeyEventType.SADD;
import static com.redis.spring.batch.reader.KeyEventType.SDIFFSTORE;
import static com.redis.spring.batch.reader.KeyEventType.SET;
import static com.redis.spring.batch.reader.KeyEventType.SETRANGE;
import static com.redis.spring.batch.reader.KeyEventType.SINTERSTORE;
import static com.redis.spring.batch.reader.KeyEventType.SORTSTORE;
import static com.redis.spring.batch.reader.KeyEventType.SPOP;
import static com.redis.spring.batch.reader.KeyEventType.SUNIONSTORE;
import static com.redis.spring.batch.reader.KeyEventType.TS_ADD;
import static com.redis.spring.batch.reader.KeyEventType.UNKNOWN;
import static com.redis.spring.batch.reader.KeyEventType.XADD;
import static com.redis.spring.batch.reader.KeyEventType.XDEL;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_CREATE;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_CREATECONSUMER;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_DELCONSUMER;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_DESTROY;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_SETID;
import static com.redis.spring.batch.reader.KeyEventType.XSETID;
import static com.redis.spring.batch.reader.KeyEventType.XTRIM;
import static com.redis.spring.batch.reader.KeyEventType.ZADD;
import static com.redis.spring.batch.reader.KeyEventType.ZDIFFSTORE;
import static com.redis.spring.batch.reader.KeyEventType.ZINCR;
import static com.redis.spring.batch.reader.KeyEventType.ZINTERSTORE;
import static com.redis.spring.batch.reader.KeyEventType.ZREM;
import static com.redis.spring.batch.reader.KeyEventType.ZREMBYRANK;
import static com.redis.spring.batch.reader.KeyEventType.ZREMBYSCORE;
import static com.redis.spring.batch.reader.KeyEventType.ZUNIONSTORE;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;

public class KeyspaceNotificationComparator implements Comparator<KeyspaceNotification> {

	private static final KeyEventType[] EVENT_TYPES = {
			// Delete
			DEL, EXPIRED, EVICTED, EXPIRE,
			// Create
			PERSIST, NEW_KEY, RESTORE, RENAME_FROM, RENAME_TO, MOVE_FROM, MOVE_TO, COPY_TO,
			// String
			SET, SETRANGE, INCRBY, INCRBYFLOAT, APPEND,
			// Hash
			HSET, HINCRBY, HINCRBYFLOAT, HDEL,
			// JSON
			JSON_SET,
			// List
			LPUSH, RPUSH, RPOP, LPOP, LINSERT, LSET, LREM, LTRIM, SORTSTORE,
			// Set
			SADD, SPOP, SINTERSTORE, SUNIONSTORE, SDIFFSTORE,
			// Sorted Set
			ZINCR, ZADD, ZREM, ZREMBYSCORE, ZREMBYRANK, ZDIFFSTORE, ZINTERSTORE, ZUNIONSTORE,
			// Stream
			XADD, XTRIM, XDEL, XGROUP_CREATE, XGROUP_CREATECONSUMER, XGROUP_DELCONSUMER, XGROUP_DESTROY, XGROUP_SETID,
			XSETID,
			// TimeSeries
			TS_ADD,
			// Other
			UNKNOWN };

	private final Map<KeyEventType, Integer> ranking;

	public KeyspaceNotificationComparator() {
		this.ranking = new EnumMap<>(KeyEventType.class);
		for (int index = 0; index < EVENT_TYPES.length; index++) {
			ranking.put(EVENT_TYPES[index], index);
		}
	}

	@Override
	public int compare(KeyspaceNotification o1, KeyspaceNotification o2) {
		return ranking.getOrDefault(o1.getEventType(), Integer.MAX_VALUE)
				- ranking.getOrDefault(o2.getEventType(), Integer.MAX_VALUE);
	}

}