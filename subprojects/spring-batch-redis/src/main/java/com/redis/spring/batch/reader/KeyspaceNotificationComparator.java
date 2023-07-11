package com.redis.spring.batch.reader;

import static com.redis.spring.batch.reader.KeyEvent.APPEND;
import static com.redis.spring.batch.reader.KeyEvent.COPY_TO;
import static com.redis.spring.batch.reader.KeyEvent.DEL;
import static com.redis.spring.batch.reader.KeyEvent.EVICTED;
import static com.redis.spring.batch.reader.KeyEvent.EXPIRE;
import static com.redis.spring.batch.reader.KeyEvent.EXPIRED;
import static com.redis.spring.batch.reader.KeyEvent.HDEL;
import static com.redis.spring.batch.reader.KeyEvent.HINCRBY;
import static com.redis.spring.batch.reader.KeyEvent.HINCRBYFLOAT;
import static com.redis.spring.batch.reader.KeyEvent.HSET;
import static com.redis.spring.batch.reader.KeyEvent.INCRBY;
import static com.redis.spring.batch.reader.KeyEvent.INCRBYFLOAT;
import static com.redis.spring.batch.reader.KeyEvent.JSON_SET;
import static com.redis.spring.batch.reader.KeyEvent.LINSERT;
import static com.redis.spring.batch.reader.KeyEvent.LPOP;
import static com.redis.spring.batch.reader.KeyEvent.LPUSH;
import static com.redis.spring.batch.reader.KeyEvent.LREM;
import static com.redis.spring.batch.reader.KeyEvent.LSET;
import static com.redis.spring.batch.reader.KeyEvent.LTRIM;
import static com.redis.spring.batch.reader.KeyEvent.MOVE_FROM;
import static com.redis.spring.batch.reader.KeyEvent.MOVE_TO;
import static com.redis.spring.batch.reader.KeyEvent.NEW_KEY;
import static com.redis.spring.batch.reader.KeyEvent.PERSIST;
import static com.redis.spring.batch.reader.KeyEvent.RENAME_FROM;
import static com.redis.spring.batch.reader.KeyEvent.RENAME_TO;
import static com.redis.spring.batch.reader.KeyEvent.RESTORE;
import static com.redis.spring.batch.reader.KeyEvent.RPOP;
import static com.redis.spring.batch.reader.KeyEvent.RPUSH;
import static com.redis.spring.batch.reader.KeyEvent.SADD;
import static com.redis.spring.batch.reader.KeyEvent.SDIFFSTORE;
import static com.redis.spring.batch.reader.KeyEvent.SET;
import static com.redis.spring.batch.reader.KeyEvent.SETRANGE;
import static com.redis.spring.batch.reader.KeyEvent.SINTERSTORE;
import static com.redis.spring.batch.reader.KeyEvent.SORTSTORE;
import static com.redis.spring.batch.reader.KeyEvent.SPOP;
import static com.redis.spring.batch.reader.KeyEvent.SUNIONSTORE;
import static com.redis.spring.batch.reader.KeyEvent.TS_ADD;
import static com.redis.spring.batch.reader.KeyEvent.UNKNOWN;
import static com.redis.spring.batch.reader.KeyEvent.XADD;
import static com.redis.spring.batch.reader.KeyEvent.XDEL;
import static com.redis.spring.batch.reader.KeyEvent.XGROUP_CREATE;
import static com.redis.spring.batch.reader.KeyEvent.XGROUP_CREATECONSUMER;
import static com.redis.spring.batch.reader.KeyEvent.XGROUP_DELCONSUMER;
import static com.redis.spring.batch.reader.KeyEvent.XGROUP_DESTROY;
import static com.redis.spring.batch.reader.KeyEvent.XGROUP_SETID;
import static com.redis.spring.batch.reader.KeyEvent.XSETID;
import static com.redis.spring.batch.reader.KeyEvent.XTRIM;
import static com.redis.spring.batch.reader.KeyEvent.ZADD;
import static com.redis.spring.batch.reader.KeyEvent.ZDIFFSTORE;
import static com.redis.spring.batch.reader.KeyEvent.ZINCR;
import static com.redis.spring.batch.reader.KeyEvent.ZINTERSTORE;
import static com.redis.spring.batch.reader.KeyEvent.ZREM;
import static com.redis.spring.batch.reader.KeyEvent.ZREMBYRANK;
import static com.redis.spring.batch.reader.KeyEvent.ZREMBYSCORE;
import static com.redis.spring.batch.reader.KeyEvent.ZUNIONSTORE;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;

public class KeyspaceNotificationComparator implements Comparator<KeyspaceNotification> {

	private static final KeyEvent[] EVENT_TYPES = {
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

	private final Map<KeyEvent, Integer> ranking;

	public KeyspaceNotificationComparator() {
		this.ranking = new EnumMap<>(KeyEvent.class);
		for (int index = 0; index < EVENT_TYPES.length; index++) {
			ranking.put(EVENT_TYPES[index], index);
		}
	}

	@Override
	public int compare(KeyspaceNotification o1, KeyspaceNotification o2) {
		return ranking.getOrDefault(o1.getEvent(), Integer.MAX_VALUE)
				- ranking.getOrDefault(o2.getEvent(), Integer.MAX_VALUE);
	}

}