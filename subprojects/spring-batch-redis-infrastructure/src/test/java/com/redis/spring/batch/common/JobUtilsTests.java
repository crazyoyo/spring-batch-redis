package com.redis.spring.batch.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;

import com.redis.spring.batch.JobUtils;

class JobUtilsTests {

	@Test
	void testCheckExecution() throws JobExecutionException {
		JobExecution jobExecution = new JobExecution(10L);
		jobExecution.setExitStatus(ExitStatus.FAILED);
		Assertions.assertThrows(JobExecutionException.class, () -> JobUtils.checkJobExecution(jobExecution));
		Assertions.assertEquals(10L, jobExecution.getId());
	}

}
