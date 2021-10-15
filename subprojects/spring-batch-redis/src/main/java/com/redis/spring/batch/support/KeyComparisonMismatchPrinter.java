package com.redis.spring.batch.support;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
@Slf4j
public class KeyComparisonMismatchPrinter<K> implements KeyComparisonItemWriter.KeyComparisonResultHandler<K> {

	@Override
	public void accept(DataStructure<K> source, DataStructure<K> target, KeyComparisonItemWriter.Status status) {
		switch (status) {
		case MISSING:
			log.warn("Missing key '{}'", source.getKey());
			break;
		case TTL:
			log.warn("TTL mismatch for key '{}': {} <> {}", source.getKey(), source.getAbsoluteTTL(),
					target.getAbsoluteTTL());
			break;
		case TYPE:
			log.warn("Type mismatch for key '{}': {} <> {}", source.getKey(), source.getType(), target.getType());
			break;
		case VALUE:
			switch (source.getType()) {
			case DataStructure.SET:
				showSetDiff(source, target);
				break;
			case DataStructure.LIST:
				showListDiff(source, target);
				break;
			case DataStructure.ZSET:
				showSortedSetDiff(source, target);
				break;
			case DataStructure.STREAM:
				showStreamDiff(source, target);
				break;
			case DataStructure.STRING:
				showStringDiff(source, target);
				break;
			case DataStructure.HASH:
				showHashDiff(source, target);
				break;
			}
			break;
		case OK:
			break;
		}
	}

	private void showHashDiff(DataStructure<K> source, DataStructure<K> target) {
		Map<String, String> sourceHash = (Map<String, String>) source.getValue();
		Map<String, String> targetHash = (Map<String, String>) target.getValue();
		Map<String, String> diff = new HashMap<>();
		diff.putAll(sourceHash);
		diff.putAll(targetHash);
		diff.entrySet()
				.removeAll(sourceHash.size() <= targetHash.size() ? sourceHash.entrySet() : targetHash.entrySet());
		log.warn("Mismatch for hash '{}' on fields: {}", source.getKey(), diff.keySet());
	}

	private void showStringDiff(DataStructure<K> source, DataStructure<K> target) {
		String sourceString = (String) source.getValue();
		String targetString = (String) target.getValue();
		int diffIndex = StringUtils.indexOfDifference(sourceString, targetString);
		log.warn("Mismatch for string '{}' at offset {}: {} <> {}", source.getKey(), diffIndex,
				substring(sourceString, diffIndex), substring(targetString, diffIndex));
	}

	private String substring(String string, int index) {
		return string.substring(Math.max(0, index - 5), Math.min(string.length(), index + 5));
	}

	private void showListDiff(DataStructure<K> source, DataStructure<K> target) {
		List<String> sourceList = (List<String>) source.getValue();
		List<String> targetList = (List<String>) target.getValue();
		if (sourceList.size() != targetList.size()) {
			log.warn("Size mismatch for list '{}': {} <> {}", source.getKey(), sourceList.size(), targetList.size());
			return;
		}
		List<String> diff = new ArrayList<>();
		for (int index = 0; index < sourceList.size(); index++) {
			if (!sourceList.get(index).equals(targetList.get(index))) {
				diff.add(String.valueOf(index));
			}
		}
		log.warn("Mismatch for list '{}' at indexes {}", source.getKey(), diff);
	}

	private void showSetDiff(DataStructure<K> source, DataStructure<K> target) {
		Set<String> sourceSet = (Set<String>) source.getValue();
		Set<String> targetSet = (Set<String>) target.getValue();
		Set<String> missing = new HashSet<>(sourceSet);
		missing.removeAll(targetSet);
		Set<String> extra = new HashSet<>(targetSet);
		extra.removeAll(sourceSet);
		log.warn("Mismatch for set '{}': {} <> {}", source.getKey(), missing, extra);
	}

	private void showSortedSetDiff(DataStructure<K> source, DataStructure<K> target) {
		List<ScoredValue<String>> sourceList = (List<ScoredValue<String>>) source.getValue();
		List<ScoredValue<String>> targetList = (List<ScoredValue<String>>) target.getValue();
		List<ScoredValue<String>> missing = new ArrayList<>(sourceList);
		missing.removeAll(targetList);
		List<ScoredValue<String>> extra = new ArrayList<>(targetList);
		extra.removeAll(sourceList);
		log.warn("Mismatch for sorted set '{}': {} <> {}", source.getKey(), print(missing), print(extra));
	}

	private List<String> print(List<ScoredValue<String>> list) {
		return list.stream().map(v -> v.getValue() + "@" + v.getScore()).collect(Collectors.toList());
	}

	private void showStreamDiff(DataStructure<K> source, DataStructure<K> target) {
		List<StreamMessage<String, String>> sourceMessages = (List<StreamMessage<String, String>>) source.getValue();
		List<StreamMessage<String, String>> targetMessages = (List<StreamMessage<String, String>>) target.getValue();
		List<StreamMessage<String, String>> missing = new ArrayList<>(sourceMessages);
		missing.removeAll(targetMessages);
		List<StreamMessage<String, String>> extra = new ArrayList<>(targetMessages);
		extra.removeAll(sourceMessages);
		log.warn("Mismatch for stream '{}': {} <> {}", source.getKey(),
				missing.stream().map(StreamMessage::getId).collect(Collectors.toList()),
				extra.stream().map(StreamMessage::getId).collect(Collectors.toList()));
	}

}