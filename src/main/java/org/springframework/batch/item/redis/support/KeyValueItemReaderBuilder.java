package org.springframework.batch.item.redis.support;

import java.util.function.Function;
import java.util.regex.Pattern;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class KeyValueItemReaderBuilder<K, V, B extends KeyValueItemReaderBuilder<K, V, B, T>, T>
	extends RedisConnectionBuilder<K, V, B> {

    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final String DEFAULT_SCAN_MATCH = "*";
    public static final int DEFAULT_CAPACITY = 10000;
    public static final long DEFAULT_POLLING_TIMEOUT = 100;
    public static final int DEFAULT_KEY_SAMPLE_SIZE = 100;

    protected int threadCount = DEFAULT_THREAD_COUNT;
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected int queueCapacity = DEFAULT_CAPACITY;
    protected long queuePollingTimeout = DEFAULT_POLLING_TIMEOUT;
    protected long scanCount = DEFAULT_SCAN_COUNT;
    protected String scanMatch = DEFAULT_SCAN_MATCH;
    private int keySampleSize = DEFAULT_KEY_SAMPLE_SIZE;
    private boolean live;
    protected final Function<B, K> pubSubPatternProvider;
    protected final Function<B, Filter<K>> keyFilterProvider;
    protected final Converter<K, K> keyExtractor;

    public KeyValueItemReaderBuilder(RedisCodec<K, V> codec, Function<B, K> pubSubPatternProvider,
	    Function<B, Filter<K>> keyFilterProvider, Converter<K, K> keyExtractor) {
	super(codec);
	this.pubSubPatternProvider = pubSubPatternProvider;
	this.keyFilterProvider = keyFilterProvider;
	this.keyExtractor = keyExtractor;
    }

    public B threads(int threads) {
	this.threadCount = threads;
	return (B) this;
    }

    public B batch(int batch) {
	this.batchSize = batch;
	return (B) this;
    }

    public B queueCapacity(int queueCapacity) {
	this.queueCapacity = queueCapacity;
	return (B) this;
    }

    public B queuePollingTimeout(long queuePollingTimeout) {
	this.queuePollingTimeout = queuePollingTimeout;
	return (B) this;
    }

    public B scanCount(long scanCount) {
	this.scanCount = scanCount;
	return (B) this;
    }

    public B scanMatch(String scanMatch) {
	this.scanMatch = scanMatch;
	return (B) this;
    }

    public B keySampleSize(int keySampleSize) {
	this.keySampleSize = keySampleSize;
	return (B) this;
    }

    public B live(boolean live) {
	this.live = live;
	return (B) this;
    }

    protected KeyItemReader<K, V> keyReader(Function<B, K> pubSubPatternProvider,
	    Function<B, Filter<K>> keyFilterProvider, Converter<K, K> keyExtractor) {
	if (live) {
	    return new LiveKeyItemReader<>(connection(), async(), timeout(), scanCount, scanMatch, keySampleSize,
		    keyFilterProvider.apply((B) this), pubSubConnection(), queueCapacity, queuePollingTimeout,
		    pubSubPatternProvider.apply((B) this), keyExtractor);
	}
	return new KeyItemReader<>(connection(), async(), timeout(), scanCount, scanMatch, keySampleSize,
		keyFilterProvider.apply((B) this));
    }

    public static <B extends KeyValueItemReaderBuilder<String, String, B, ?>> Function<B, String> stringPubSubPatternProvider() {
	return b -> "__keyspace@" + b.uri().getDatabase() + "__:" + b.scanMatch;
    }

    public static <B extends KeyValueItemReaderBuilder<String, String, B, ?>> Function<B, Filter<String>> keyFilterProvider() {
	return b -> new KeyFilter(Pattern.compile(convertGlobToRegex(b.scanMatch)));
    }

    /**
     * Converts a standard POSIX Shell globbing pattern into a regular expression
     * pattern. The result can be used with the standard {@link java.util.regex} API
     * to recognize strings which match the glob pattern.
     * See also, the POSIX Shell language:
     * http://pubs.opengroup.org/onlinepubs/009695399/utilities/xcu_chap02.html#tag_02_13_01
     * 
     * @param pattern A glob pattern.
     * @return A regex pattern to recognize the given glob pattern.
     */
    public static final String convertGlobToRegex(String pattern) {
	StringBuilder sb = new StringBuilder(pattern.length());
	int inGroup = 0;
	int inClass = 0;
	int firstIndexInClass = -1;
	char[] arr = pattern.toCharArray();
	for (int i = 0; i < arr.length; i++) {
	    char ch = arr[i];
	    switch (ch) {
	    case '\\':
		if (++i >= arr.length) {
		    sb.append('\\');
		} else {
		    char next = arr[i];
		    switch (next) {
		    case ',':
			// escape not needed
			break;
		    case 'Q':
		    case 'E':
			// extra escape needed
			sb.append('\\');
		    default:
			sb.append('\\');
		    }
		    sb.append(next);
		}
		break;
	    case '*':
		if (inClass == 0)
		    sb.append(".*");
		else
		    sb.append('*');
		break;
	    case '?':
		if (inClass == 0)
		    sb.append('.');
		else
		    sb.append('?');
		break;
	    case '[':
		inClass++;
		firstIndexInClass = i + 1;
		sb.append('[');
		break;
	    case ']':
		inClass--;
		sb.append(']');
		break;
	    case '.':
	    case '(':
	    case ')':
	    case '+':
	    case '|':
	    case '^':
	    case '$':
	    case '@':
	    case '%':
		if (inClass == 0 || (firstIndexInClass == i && ch == '^'))
		    sb.append('\\');
		sb.append(ch);
		break;
	    case '!':
		if (firstIndexInClass == i)
		    sb.append('^');
		else
		    sb.append('!');
		break;
	    case '{':
		inGroup++;
		sb.append('(');
		break;
	    case '}':
		inGroup--;
		sb.append(')');
		break;
	    case ',':
		if (inGroup > 0)
		    sb.append('|');
		else
		    sb.append(',');
		break;
	    default:
		sb.append(ch);
	    }
	}
	return sb.toString();
    }

}
