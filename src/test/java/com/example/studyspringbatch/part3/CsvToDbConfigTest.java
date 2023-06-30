package com.example.studyspringbatch.part3;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBatchTest
//@SpringBootTest
@ContextConfiguration(classes = { JobTestConfiguration.class, CsvToDbConfig.class} )
public class CsvToDbConfigTest {

	@Autowired
	private JobLauncherTestUtils launcherTestUtils;

	@Autowired
	private PersonRepository personRepository;

	@AfterEach
	public void tearDown(){
		personRepository.deleteAll();
	}


	@Test
	public void test_step(){
		JobExecution execution = launcherTestUtils.launchStep("csvToDbStep");
		Assertions.assertThat(
				execution.getStepExecutions().stream()
					.mapToInt(StepExecution::getWriteCount)
					.sum()
			)
			.isEqualTo(personRepository.count())
			.isEqualTo(3);
	}

	@Test
	public void test_allow_duplicate() throws Exception {
		JobParameters jobParameters = new JobParametersBuilder()
			.addString("allowDuplicate", "false")
			.toJobParameters();

		JobExecution jobExecution = launcherTestUtils.launchJob(jobParameters);

		Assertions.assertThat(
			jobExecution.getStepExecutions().stream()
			.mapToInt(StepExecution::getWriteCount)
			.sum()
		)
			.isEqualTo(personRepository.count())
			.isEqualTo(3);
	}

	@Test
	public void test_not_allow_duplicate() throws Exception {
		JobParameters jobParameters = new JobParametersBuilder()
			.addString("allowDuplicate", "true")
			.toJobParameters();

		JobExecution jobExecution = launcherTestUtils.launchJob(jobParameters);


		Assertions.assertThat(
				jobExecution.getStepExecutions().stream()
					.mapToInt(StepExecution::getWriteCount)
					.sum()
			)
			.isEqualTo(personRepository.count())
			.isEqualTo(100);
	}
}