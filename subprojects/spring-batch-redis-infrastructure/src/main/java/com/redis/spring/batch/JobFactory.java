package com.redis.spring.batch;

import java.util.Optional;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties.Jdbc;
import org.springframework.boot.jdbc.init.DataSourceScriptDatabaseInitializer;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

public class JobFactory implements JobLauncher, InitializingBean {

	private static final String STEP_ERROR = "Error executing step %s: %s";

	private JobRepository jobRepository;
	private PlatformTransactionManager platformTransactionManager;
	private JobLauncher jobLauncher;
	private JobLauncher asyncJobLauncher;
	private String name = ClassUtils.getShortName(getClass());

	@Override
	public void afterPropertiesSet() throws Exception {
		if (platformTransactionManager == null) {
			platformTransactionManager = resourcelessTransactionManager();
		}
		if (jobRepository == null) {
			jobRepository = jobRepository();
			if (jobRepository == null) {
				throw new IllegalStateException("Could not initialize job repository");
			}
		}
		if (jobLauncher == null) {
			jobLauncher = jobLauncher(new SyncTaskExecutor());
			asyncJobLauncher = jobLauncher(new SimpleAsyncTaskExecutor());
		}
	}

	public static ResourcelessTransactionManager resourcelessTransactionManager() {
		return new ResourcelessTransactionManager();
	}

	private JobRepository jobRepository() throws Exception {
		JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
		bean.setDataSource(dataSource(name));
		bean.setDatabaseType("HSQL");
		bean.setTransactionManager(platformTransactionManager);
		bean.afterPropertiesSet();
		return bean.getObject();
	}

	private static JDBCDataSource dataSource(String name) throws Exception {
		JDBCDataSource source = new JDBCDataSource();
		source.setURL("jdbc:hsqldb:mem:" + name);
		Jdbc jdbc = new Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		DataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(source, jdbc);
		initializer.afterPropertiesSet();
		initializer.initializeDatabase();
		return source;
	}

	private JobLauncher jobLauncher(TaskExecutor taskExecutor) {
		TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(taskExecutor);
		return launcher;
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize) {
		return stepBuilder(name).chunk(chunkSize, platformTransactionManager);
	}

	public StepBuilder stepBuilder(String name) {
		return new StepBuilder(name, jobRepository);
	}

	public JobExecution run(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return run(job, new JobParameters());
	}

	public JobExecution runAsync(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return runAsync(job, new JobParameters());
	}

	@Override
	public JobExecution run(Job job, JobParameters jobParameters) throws JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return jobLauncher.run(job, jobParameters);
	}

	public JobExecution runAsync(Job job, JobParameters jobParameters) throws JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return asyncJobLauncher.run(job, jobParameters);
	}

	public static JobExecution checkJobExecution(JobExecution jobExecution) throws JobExecutionException {
		if (isFailed(jobExecution.getExitStatus())) {
			Optional<JobExecutionException> stepExecutionException = jobExecution.getStepExecutions().stream()
					.filter(e -> isFailed(e.getExitStatus())).map(JobFactory::stepExecutionException).findAny();
			if (stepExecutionException.isPresent()) {
				throw stepExecutionException.get();
			}
			if (jobExecution.getAllFailureExceptions().isEmpty()) {
				throw new JobExecutionException(String.format("Error executing job %s: %s",
						jobExecution.getJobInstance(), jobExecution.getExitStatus().getExitDescription()));
			}
		}
		return jobExecution;
	}

	private static JobExecutionException stepExecutionException(StepExecution stepExecution) {
		String message = String.format(STEP_ERROR, stepExecution.getStepName(),
				stepExecution.getExitStatus().getExitDescription());
		if (stepExecution.getFailureExceptions().isEmpty()) {
			return new JobExecutionException(message);
		}
		return new JobExecutionException(message, stepExecution.getFailureExceptions().get(0));
	}

	private static boolean isFailed(ExitStatus exitStatus) {
		return exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode());
	}

	public JobBuilder jobBuilder(String name) {
		return new JobBuilder(name, jobRepository);
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setPlatformTransactionManager(PlatformTransactionManager platformTransactionManager) {
		this.platformTransactionManager = platformTransactionManager;
	}

	public void setJobLauncher(JobLauncher jobLauncher) {
		this.jobLauncher = jobLauncher;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JobLauncher getJobLauncher() {
		return jobLauncher;
	}

	public JobLauncher getAsyncJobLauncher() {
		return asyncJobLauncher;
	}

	public PlatformTransactionManager getPlatformTransactionManager() {
		return platformTransactionManager;
	}

	public JobRepository getJobRepository() {
		return jobRepository;
	}

}
