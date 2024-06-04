package com.redis.spring.batch;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties.Jdbc;
import org.springframework.boot.jdbc.init.DataSourceScriptDatabaseInitializer;
import org.springframework.boot.sql.init.DatabaseInitializationMode;

public abstract class JobUtils {

	private static final String STEP_ERROR = "Error executing step %s: %s";

	private JobUtils() {
	}

	public static ResourcelessTransactionManager resourcelessTransactionManager() {
		return new ResourcelessTransactionManager();
	}

	public static JobRepositoryFactoryBean jobRepositoryFactoryBean(String name) throws Exception {
		JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
		bean.setDataSource(hsqldbDataSource(name));
		bean.setDatabaseType("HSQL");
		bean.setTransactionManager(resourcelessTransactionManager());
		bean.afterPropertiesSet();
		return bean;
	}

	public static JDBCDataSource hsqldbDataSource(String databaseName) throws Exception {
		JDBCDataSource source = new JDBCDataSource();
		source.setURL("jdbc:hsqldb:mem:" + databaseName);
		Jdbc jdbc = new Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		DataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(source, jdbc);
		initializer.afterPropertiesSet();
		initializer.initializeDatabase();
		return source;
	}

	public static JobExecution checkJobExecution(JobExecution jobExecution) throws JobExecutionException {
		if (isFailed(jobExecution.getExitStatus())) {
			Optional<JobExecutionException> stepExecutionException = jobExecution.getStepExecutions().stream()
					.filter(e -> isFailed(e.getExitStatus())).map(JobUtils::stepExecutionException).findAny();
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

	public static <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> faultTolerantStep = step.faultTolerant();
		faultTolerantStep.skip(ExecutionException.class);
		faultTolerantStep.noRetry(ExecutionException.class);
		faultTolerantStep.noSkip(TimeoutException.class);
		faultTolerantStep.retry(TimeoutException.class);
		return faultTolerantStep;
	}

}
