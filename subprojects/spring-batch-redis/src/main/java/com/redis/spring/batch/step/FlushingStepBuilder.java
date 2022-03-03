package com.redis.spring.batch.step;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.step.builder.FlowStepBuilder;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.builder.PartitionStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.CompletionPolicy;

public class FlushingStepBuilder extends StepBuilderHelper<FlushingStepBuilder> {

	public FlushingStepBuilder(String name) {
		super(name);
	}

	public TaskletStepBuilder tasklet(Tasklet tasklet) {
		return new TaskletStepBuilder(this).tasklet(tasklet);
	}

	public <I, O> FlushingSimpleStepBuilder<I, O> chunk(int chunkSize) {
		return new FlushingSimpleStepBuilder<I, O>(this).chunk(chunkSize);
	}

	public <I, O> FlushingSimpleStepBuilder<I, O> chunk(CompletionPolicy completionPolicy) {
		return new FlushingSimpleStepBuilder<I, O>(this).chunk(completionPolicy);
	}

	public PartitionStepBuilder partitioner(String stepName, Partitioner partitioner) {
		return new PartitionStepBuilder(this).partitioner(stepName, partitioner);
	}

	public PartitionStepBuilder partitioner(Step step) {
		return new PartitionStepBuilder(this).step(step);
	}

	public JobStepBuilder job(Job job) {
		return new JobStepBuilder(this).job(job);
	}

	public FlowStepBuilder flow(Flow flow) {
		return new FlowStepBuilder(this).flow(flow);
	}

}
