package org.springframework.batch.item.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings({"unused", "BusyWait", "SingleStatementInBlock", "NullableProblems", "SameParameterValue"})
public abstract class AbstractTestBase {

    @Autowired
    protected JobLauncher jobLauncher;
    @Autowired
    protected JobLauncher asyncJobLauncher;
    @Autowired
    protected JobBuilderFactory jobs;
    @Autowired
    protected StepBuilderFactory steps;

    protected Job job(String name, TaskletStep step) {
        return jobs.get(name + "-job").start(step).build();
    }

    protected <T> JobExecution execute(String name, ItemReader<? extends T> reader, ItemWriter<T> writer) throws Exception {
        return execute(name, reader, null, writer);
    }

    protected <I, O> JobExecution execute(String name, ItemReader<? extends I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) throws Exception {
        return execute(name, step(name, reader, processor, writer).build());
    }

    protected JobExecution execute(String name, TaskletStep step) throws Exception {
        return checkForFailure(jobLauncher.run(job(name, step), new JobParameters()));
    }

    protected JobExecution checkForFailure(JobExecution execution) {
        if (!execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
            Assertions.fail("Job not completed: " + execution.getExitStatus());
        }
        return execution;
    }

    protected <I, O> JobExecution executeFlushing(String name, PollableItemReader<? extends I> reader, ItemWriter<O> writer) throws Exception {
        return executeFlushing(name, reader, null, writer);
    }

    protected <I, O> JobExecution executeFlushing(String name, PollableItemReader<? extends I> reader, ItemProcessor<I,O> processor, ItemWriter<O> writer) throws Exception {
        TaskletStep step = flushing(step(name, reader,processor, writer)).build();
        JobExecution execution = asyncJobLauncher.run(job(name, step), new JobParameters());
        awaitRunning(execution);
        Thread.sleep(200);
        return execution;
    }

    protected <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader, ItemWriter<O> writer) {
        return step(name, reader, null, writer);
    }

    protected <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader, ItemProcessor<I,O> processor, ItemWriter<O> writer) {
        return steps.get(name + "-step").<I, O>chunk(50).reader(reader).processor(processor).writer(writer);
    }

    protected <I, O> FlushingStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step) {
        return new FlushingStepBuilder<>(step).idleTimeout(Duration.ofMillis(500));
    }

    private static class MapFieldSetMapper implements FieldSetMapper<Map<String, String>> {

        @Override
        public Map<String, String> mapFieldSet(FieldSet fieldSet) {
            Map<String, String> fields = new HashMap<>();
            String[] names = fieldSet.getNames();
            for (int index = 0; index < names.length; index++) {
                String name = names[index];
                String value = fieldSet.readString(index);
                if (value == null || value.length() == 0) {
                    continue;
                }
                fields.put(name, value);
            }
            return fields;
        }

    }

    protected FlatFileItemReader<Map<String, String>> fileReader(Resource resource) throws IOException {
        FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
        builder.name("flat-file-reader");
        builder.resource(resource);
        builder.saveState(false);
        builder.linesToSkip(1);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
        BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, FlatFileItemReader.DEFAULT_CHARSET);
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_COMMA);
        String[] fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
        delimitedBuilder.names(fieldNames);
        return builder.build();
    }

    protected void awaitJobTermination(JobExecution execution) throws Exception {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
        checkForFailure(execution);
    }

    protected static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

        private final List<T> writtenItems = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void write(List<? extends T> items) {
            writtenItems.addAll(items);
        }

        public List<? extends T> getWrittenItems() {
            return this.writtenItems;
        }
    }

    protected void awaitRunning(JobExecution execution) throws InterruptedException {
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
    }

}
