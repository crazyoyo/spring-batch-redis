package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;

@SuppressWarnings("deprecation")
public class JobFactory implements InitializingBean {

    @Getter
    private JobBuilderFactory jobBuilderFactory;
    @Getter
    private StepBuilderFactory stepBuilderFactory;
    @Getter
    private SimpleJobLauncher syncLauncher;
    @Getter
    private SimpleJobLauncher asyncLauncher;

    @SuppressWarnings("ConstantConditions")
    @Override
    public void afterPropertiesSet() throws Exception {
        MapJobRepositoryFactoryBean jobRepositoryFactoryBean = new MapJobRepositoryFactoryBean();
        JobRepository jobRepository = jobRepositoryFactoryBean.getObject();
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, jobRepositoryFactoryBean.getTransactionManager());
        syncLauncher = new SimpleJobLauncher();
        syncLauncher.setJobRepository(jobRepository);
        syncLauncher.setTaskExecutor(new SyncTaskExecutor());
        syncLauncher.afterPropertiesSet();
        asyncLauncher = new SimpleJobLauncher();
        asyncLauncher.setJobRepository(jobRepository);
        asyncLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        asyncLauncher.afterPropertiesSet();
    }

}
