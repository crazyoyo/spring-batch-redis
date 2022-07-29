package com.redis.spring.batch.compare;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class KeyComparisonLogger implements KeyComparisonListener {

	/**
	 * Represents a failed index search.
	 * 
	 */
	public static final int INDEX_NOT_FOUND = -1;

	private final Logger log;

	public KeyComparisonLogger() {
		this(LoggerFactory.getLogger(KeyComparisonLogger.class));
	}

	public KeyComparisonLogger(Logger logger) {
		this.log = logger;
	}

	@Override
	public void keyComparison(KeyComparison comparison) {
		switch (comparison.getStatus()) {
		case MISSING:
			log.warn("Missing key '{}'", comparison.getSource().getKey());
			break;
		case TTL:
			log.warn("TTL mismatch for key '{}': {} <> {}", comparison.getSource().getKey(),
					comparison.getSource().getTtl(), comparison.getTarget().getTtl());
			break;
		case TYPE:
			log.warn("Type mismatch for key '{}': {} <> {}", comparison.getSource().getKey(),
					comparison.getSource().getType(), comparison.getTarget().getType());
			break;
		case VALUE:
			switch (comparison.getSource().getType()) {
			case SET:
				showSetDiff(comparison);
				break;
			case LIST:
				showListDiff(comparison);
				break;
			case ZSET:
				showSortedSetDiff(comparison);
				break;
			case STREAM:
				showStreamDiff(comparison);
				break;
			case STRING:
			case JSON:
				showStringDiff(comparison);
				break;
			case HASH:
				showHashDiff(comparison);
				break;
			case TIMESERIES:
				showListDiff(comparison);
				break;
			default:
				log.warn("Value mismatch for key '{}'", comparison.getSource().getKey());
				break;
			}
			break;
		case OK:
			break;
		}

	}

	private void showHashDiff(KeyComparison comparison) {
		Map<String, String> sourceHash = (Map<String, String>) comparison.getSource().getValue();
		Map<String, String> targetHash = (Map<String, String>) comparison.getTarget().getValue();
		Map<String, String> diff = new HashMap<>();
		diff.putAll(sourceHash);
		diff.putAll(targetHash);
		diff.entrySet()
				.removeAll(sourceHash.size() <= targetHash.size() ? sourceHash.entrySet() : targetHash.entrySet());
		log.warn("Value mismatch for hash '{}' on fields: {}", comparison.getSource().getKey(), diff.keySet());
	}

	private void showStringDiff(KeyComparison comparison) {
		String sourceString = (String) comparison.getSource().getValue();
		String targetString = (String) comparison.getTarget().getValue();
		int diffIndex = indexOfDifference(sourceString, targetString);
		log.warn("Value mismatch for string '{}' at offset {}", comparison.getSource().getKey(), diffIndex);
	}

	/**
	 * <p>
	 * Compares two CharSequences, and returns the index at which the CharSequences
	 * begin to differ.
	 * </p>
	 *
	 * <p>
	 * For example, {@code indexOfDifference("i am a machine", "i am a robot") -> 7}
	 * </p>
	 *
	 * <pre>
	 * StringUtils.indexOfDifference(null, null) = -1
	 * StringUtils.indexOfDifference("", "") = -1
	 * StringUtils.indexOfDifference("", "abc") = 0
	 * StringUtils.indexOfDifference("abc", "") = 0
	 * StringUtils.indexOfDifference("abc", "abc") = -1
	 * StringUtils.indexOfDifference("ab", "abxyz") = 2
	 * StringUtils.indexOfDifference("abcde", "abxyz") = 2
	 * StringUtils.indexOfDifference("abcde", "xyz") = 0
	 * </pre>
	 *
	 * @param cs1 the first CharSequence, may be null
	 * @param cs2 the second CharSequence, may be null
	 * @return the index where cs1 and cs2 begin to differ; -1 if they are equal
	 * @since 2.0
	 * @since 3.0 Changed signature from indexOfDifference(String, String) to
	 *        indexOfDifference(CharSequence, CharSequence)
	 */
	private static int indexOfDifference(final CharSequence cs1, final CharSequence cs2) {
		if (cs1 == cs2) {
			return INDEX_NOT_FOUND;
		}
		if (cs1 == null || cs2 == null) {
			return 0;
		}
		int i;
		for (i = 0; i < cs1.length() && i < cs2.length(); ++i) {
			if (cs1.charAt(i) != cs2.charAt(i)) {
				break;
			}
		}
		if (i < cs2.length() || i < cs1.length()) {
			return i;
		}
		return INDEX_NOT_FOUND;
	}

	private void showListDiff(KeyComparison comparison) {
		List<?> sourceList = (List<?>) comparison.getSource().getValue();
		List<?> targetList = (List<?>) comparison.getTarget().getValue();
		if (sourceList.size() != targetList.size()) {
			log.warn("Size mismatch for {} '{}': {} <> {}", comparison.getSource().getType(),
					comparison.getSource().getKey(), sourceList.size(), targetList.size());
			return;
		}
		List<Integer> diff = new ArrayList<>();
		for (int index = 0; index < sourceList.size(); index++) {
			if (!sourceList.get(index).equals(targetList.get(index))) {
				diff.add(index);
			}
		}
		log.warn("Value mismatch for {} '{}' at indexes {}", comparison.getSource().getType(),
				comparison.getSource().getKey(), diff);
	}

	private void showSetDiff(KeyComparison comparison) {
		Set<String> sourceSet = (Set<String>) comparison.getSource().getValue();
		Set<String> targetSet = (Set<String>) comparison.getTarget().getValue();
		Set<String> missing = new HashSet<>(sourceSet);
		missing.removeAll(targetSet);
		Set<String> extra = new HashSet<>(targetSet);
		extra.removeAll(sourceSet);
		log.warn("Value mismatch for set '{}': {} <> {}", comparison.getSource().getKey(), missing, extra);
	}

	private void showSortedSetDiff(KeyComparison comparison) {
		List<ScoredValue<String>> sourceList = (List<ScoredValue<String>>) comparison.getSource().getValue();
		List<ScoredValue<String>> targetList = (List<ScoredValue<String>>) comparison.getTarget().getValue();
		List<ScoredValue<String>> missing = new ArrayList<>(sourceList);
		missing.removeAll(targetList);
		List<ScoredValue<String>> extra = new ArrayList<>(targetList);
		extra.removeAll(sourceList);
		log.warn("Value mismatch for sorted set '{}': {} <> {}", comparison.getSource().getKey(), print(missing),
				print(extra));
	}

	private List<String> print(List<ScoredValue<String>> list) {
		return list.stream().map(v -> v.getValue() + "@" + v.getScore()).collect(Collectors.toList());
	}

	private void showStreamDiff(KeyComparison comparison) {
		List<StreamMessage<String, String>> sourceMessages = (List<StreamMessage<String, String>>) comparison
				.getSource().getValue();
		List<StreamMessage<String, String>> targetMessages = (List<StreamMessage<String, String>>) comparison
				.getTarget().getValue();
		List<StreamMessage<String, String>> missing = new ArrayList<>(sourceMessages);
		missing.removeAll(targetMessages);
		List<StreamMessage<String, String>> extra = new ArrayList<>(targetMessages);
		extra.removeAll(sourceMessages);
		log.warn("Value mismatch for stream '{}': {} <> {}", comparison.getSource().getKey(),
				missing.stream().map(StreamMessage::getId).collect(Collectors.toList()),
				extra.stream().map(StreamMessage::getId).collect(Collectors.toList()));
	}

}