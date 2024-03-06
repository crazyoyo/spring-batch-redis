package com.redis.spring.batch.test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.TestInfo;

public class SimpleTestInfo implements TestInfo {

    private final TestInfo delegate;

    private final String[] suffixes;

    public SimpleTestInfo(TestInfo delegate, String... suffixes) {
        this.delegate = delegate;
        this.suffixes = suffixes;
    }

    @Override
    public String getDisplayName() {
        List<String> elements = new ArrayList<>();
        elements.add(delegate.getDisplayName());
        elements.addAll(Arrays.asList(suffixes));
        return String.join("-", elements);
    }

    @Override
    public Set<String> getTags() {
        return delegate.getTags();
    }

    @Override
    public Optional<Class<?>> getTestClass() {
        return delegate.getTestClass();
    }

    @Override
    public Optional<Method> getTestMethod() {
        return delegate.getTestMethod();
    }

}
