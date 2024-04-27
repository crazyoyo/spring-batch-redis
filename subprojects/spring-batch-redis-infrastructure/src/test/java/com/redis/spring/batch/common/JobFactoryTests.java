package com.redis.spring.batch.common;

import java.util.Collection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;

class JobFactoryTests {

	@Test
	void testCheckExecution() throws JobExecutionException {
		JobExecution jobExecution = new JobExecution(10L);
		jobExecution.setExitStatus(ExitStatus.FAILED);
		Assertions.assertThrows(JobExecutionException.class, () -> JobFactory.checkJobExecution(jobExecution));
		Assertions.assertEquals(10L, jobExecution.getId());
	}

	private class SimpleJobRepository implements JobRepository {

		@Override
		public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public JobExecution createJobExecution(String jobName, JobParameters jobParameters)
				throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void update(JobExecution jobExecution) {
			// TODO Auto-generated method stub

		}

		@Override
		public void add(StepExecution stepExecution) {
			// TODO Auto-generated method stub

		}

		@Override
		public void addAll(Collection<StepExecution> stepExecutions) {
			// TODO Auto-generated method stub

		}

		@Override
		public void update(StepExecution stepExecution) {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateExecutionContext(StepExecution stepExecution) {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateExecutionContext(JobExecution jobExecution) {
			// TODO Auto-generated method stub

		}

		@Override
		public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getStepExecutionCount(JobInstance jobInstance, String stepName) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
			// TODO Auto-generated method stub
			return null;
		}

	}

	@Test
	void testSetters() {
		JobFactory factory = new JobFactory();
		JobLauncher launcher = new TaskExecutorJobLauncher();
		SimpleJobRepository repository = new SimpleJobRepository();
		String name = "name";
		ResourcelessTransactionManager transactionManager = new ResourcelessTransactionManager();
		factory.setJobLauncher(launcher);
		factory.setJobRepository(repository);
		factory.setName(name);
		factory.setPlatformTransactionManager(transactionManager);
		Assertions.assertEquals(name, factory.getName());
		Assertions.assertEquals(repository, factory.getJobRepository());
		Assertions.assertEquals(launcher, factory.getJobLauncher());
		Assertions.assertEquals(transactionManager, factory.getPlatformTransactionManager());
	}

}
