package com.redis.spring.batch;

import org.junit.runner.RunWith;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.support.JobFactory;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase implements InitializingBean {

    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private PlatformTransactionManager transactionManager;

    protected JobFactory jobFactory;

    @Override
    public void afterPropertiesSet() throws Exception {
        jobFactory = new JobFactory(jobRepository, transactionManager);
    }

}
