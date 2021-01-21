package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@SuppressWarnings("deprecation")
public class JobFactory implements InitializingBean {

    private JobBuilderFactory jobs;
    private StepBuilderFactory steps;
    @Getter
    private SimpleJobLauncher syncLauncher;
    @Getter
    private SimpleJobLauncher asyncLauncher;

    @Override
    public void afterPropertiesSet() throws Exception {
        MapJobRepositoryFactoryBean jobRepositoryFactoryBean = new MapJobRepositoryFactoryBean();
        JobRepository jobRepository = jobRepositoryFactoryBean.getObject();
        jobs = new JobBuilderFactory(jobRepository);
        steps = new StepBuilderFactory(jobRepository, jobRepositoryFactoryBean.getTransactionManager());
        syncLauncher = new SimpleJobLauncher();
        syncLauncher.setJobRepository(jobRepository);
        syncLauncher.afterPropertiesSet();
        asyncLauncher = new SimpleJobLauncher();
        asyncLauncher.setJobRepository(jobRepository);
        asyncLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        asyncLauncher.afterPropertiesSet();
    }

    public StepBuilder step(String name) {
        return steps.get(name);
    }

    public JobBuilder job(String name) {
        return jobs.get(name);
    }

}
