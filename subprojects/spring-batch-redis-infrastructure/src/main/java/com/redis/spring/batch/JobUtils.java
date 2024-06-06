package com.redis.spring.batch;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
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

	public static Optional<Throwable> exception(JobExecution jobExecution) {
		for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
			if (!stepExecution.getFailureExceptions().isEmpty()) {
				return Optional.of(stepExecution.getFailureExceptions().get(0));
			}
		}
		return Optional.empty();
	}

	public static boolean isFailed(ExitStatus exitStatus) {
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
