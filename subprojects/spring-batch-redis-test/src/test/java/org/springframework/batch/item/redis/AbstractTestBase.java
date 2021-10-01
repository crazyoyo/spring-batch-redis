package org.springframework.batch.item.redis;

import org.junit.runner.RunWith;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase implements InitializingBean {

    @SuppressWarnings("unused")
    @Autowired
    private JobRepository jobRepository;
    @SuppressWarnings("unused")
    @Autowired
    private PlatformTransactionManager transactionManager;

    protected JobFactory jobFactory;

    @Override
    public void afterPropertiesSet() throws Exception {
        jobFactory = new JobFactory(jobRepository, transactionManager);
    }

}
