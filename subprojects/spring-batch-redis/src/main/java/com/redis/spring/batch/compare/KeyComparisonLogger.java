package com.redis.spring.batch.compare;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class KeyComparisonLogger implements KeyComparisonListener<String> {

	private final Logger log;

	public KeyComparisonLogger() {
		this(LoggerFactory.getLogger(KeyComparisonLogger.class));
	}

	public KeyComparisonLogger(Logger logger) {
		this.log = logger;
	}

	@Override
	public void keyComparison(KeyComparison<String> comparison) {
		switch (comparison.getStatus()) {
		case MISSING:
			log.warn("Missing key '{}'", comparison.getSource().getKey());
			break;
		case TTL:
			log.warn("TTL mismatch for key '{}': {} <> {}", comparison.getSource().getKey(),
					comparison.getSource().getAbsoluteTTL(), comparison.getTarget().getAbsoluteTTL());
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
				showStringDiff(comparison);
				break;
			case HASH:
				showHashDiff(comparison);
				break;
			default:
				break;
			}
			break;
		case OK:
			break;
		}
	}

	private void showHashDiff(KeyComparison<String> comparison) {
		Map<String, String> sourceHash = (Map<String, String>) comparison.getSource().getValue();
		Map<String, String> targetHash = (Map<String, String>) comparison.getTarget().getValue();
		Map<String, String> diff = new HashMap<>();
		diff.putAll(sourceHash);
		diff.putAll(targetHash);
		diff.entrySet()
				.removeAll(sourceHash.size() <= targetHash.size() ? sourceHash.entrySet() : targetHash.entrySet());
		log.warn("Mismatch for hash '{}' on fields: {}", comparison.getSource().getKey(), diff.keySet());
	}

	private void showStringDiff(KeyComparison<String> comparison) {
		String sourceString = (String) comparison.getSource().getValue();
		String targetString = (String) comparison.getTarget().getValue();
		int diffIndex = StringUtils.indexOfDifference(sourceString, targetString);
		log.warn("Mismatch for string '{}' at offset {}", comparison.getSource().getKey(), diffIndex);
	}

	private void showListDiff(KeyComparison<String> comparison) {
		List<String> sourceList = (List<String>) comparison.getSource().getValue();
		List<String> targetList = (List<String>) comparison.getTarget().getValue();
		if (sourceList.size() != targetList.size()) {
			log.warn("Size mismatch for list '{}': {} <> {}", comparison.getSource().getKey(), sourceList.size(),
					targetList.size());
			return;
		}
		List<String> diff = new ArrayList<>();
		for (int index = 0; index < sourceList.size(); index++) {
			if (!sourceList.get(index).equals(targetList.get(index))) {
				diff.add(String.valueOf(index));
			}
		}
		log.warn("Mismatch for list '{}' at indexes {}", comparison.getSource().getKey(), diff);
	}

	private void showSetDiff(KeyComparison<String> comparison) {
		Set<String> sourceSet = (Set<String>) comparison.getSource().getValue();
		Set<String> targetSet = (Set<String>) comparison.getTarget().getValue();
		Set<String> missing = new HashSet<>(sourceSet);
		missing.removeAll(targetSet);
		Set<String> extra = new HashSet<>(targetSet);
		extra.removeAll(sourceSet);
		log.warn("Mismatch for set '{}': {} <> {}", comparison.getSource().getKey(), missing, extra);
	}

	private void showSortedSetDiff(KeyComparison<String> comparison) {
		List<ScoredValue<String>> sourceList = (List<ScoredValue<String>>) comparison.getSource().getValue();
		List<ScoredValue<String>> targetList = (List<ScoredValue<String>>) comparison.getTarget().getValue();
		List<ScoredValue<String>> missing = new ArrayList<>(sourceList);
		missing.removeAll(targetList);
		List<ScoredValue<String>> extra = new ArrayList<>(targetList);
		extra.removeAll(sourceList);
		log.warn("Mismatch for sorted set '{}': {} <> {}", comparison.getSource().getKey(), print(missing),
				print(extra));
	}

	private List<String> print(List<ScoredValue<String>> list) {
		return list.stream().map(v -> v.getValue() + "@" + v.getScore()).collect(Collectors.toList());
	}

	private void showStreamDiff(KeyComparison<String> comparison) {
		List<StreamMessage<String, String>> sourceMessages = (List<StreamMessage<String, String>>) comparison
				.getSource().getValue();
		List<StreamMessage<String, String>> targetMessages = (List<StreamMessage<String, String>>) comparison
				.getTarget().getValue();
		List<StreamMessage<String, String>> missing = new ArrayList<>(sourceMessages);
		missing.removeAll(targetMessages);
		List<StreamMessage<String, String>> extra = new ArrayList<>(targetMessages);
		extra.removeAll(sourceMessages);
		log.warn("Mismatch for stream '{}': {} <> {}", comparison.getSource().getKey(),
				missing.stream().map(StreamMessage::getId).collect(Collectors.toList()),
				extra.stream().map(StreamMessage::getId).collect(Collectors.toList()));
	}

}