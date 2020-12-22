package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@SuppressWarnings("deprecation")
public class JobFactory implements InitializingBean {

    @Getter
    private JobBuilderFactory jobBuilderFactory;
    @Getter
    private StepBuilderFactory stepBuilderFactory;
    private JobLauncher syncLauncher;
    @Getter
    private JobLauncher asyncLauncher;

    @Override
    public void afterPropertiesSet() throws Exception {
        MapJobRepositoryFactoryBean jobRepositoryFactoryBean = new MapJobRepositoryFactoryBean();
        JobRepository jobRepository = jobRepositoryFactoryBean.getObject();
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, jobRepositoryFactoryBean.getTransactionManager());
        asyncLauncher = asyncLauncher(jobRepository);
        syncLauncher = launcher(jobRepository);
    }

    private JobLauncher launcher(JobRepository jobRepository) throws Exception {
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.afterPropertiesSet();
        return launcher;
    }

    private JobLauncher asyncLauncher(JobRepository jobRepository) throws Exception {
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        launcher.afterPropertiesSet();
        return launcher;
    }

    public void execute(Job job, JobParameters parameters) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        syncLauncher.run(job, parameters);
    }

    public JobExecution executeAsync(Job job, JobParameters parameters) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        return asyncLauncher.run(job, parameters);
    }

    public <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize, int threads) {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(threads);
        return (SimpleStepBuilder<I, O>) stepBuilderFactory.get(name).<I, O>chunk(chunkSize).taskExecutor(taskExecutor).throttleLimit(threads);
    }

}
