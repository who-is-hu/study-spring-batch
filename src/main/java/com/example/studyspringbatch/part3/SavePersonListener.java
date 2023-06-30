package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeStep;

@Slf4j
public class SavePersonListener {

	public static class SavePersonStepExecutionListener {
		@BeforeStep
		public void beforeStep(StepExecution stepExecution) {
			log.info("before step");
		}

		@AfterStep
		public ExitStatus afterStep(StepExecution stepExecution) {
			log.info("after step {}", stepExecution.getWriteCount());
			return stepExecution.getExitStatus();
		}
	}
	public static class SavePersonJobExecutionListener implements JobExecutionListener {
		@Override
		public void beforeJob(JobExecution jobExecution) {
			log.info("before job");
		}

		@Override
		public void afterJob(JobExecution jobExecution) {
			int sum = jobExecution.getStepExecutions().stream().mapToInt(StepExecution::getWriteCount).sum();
			log.info("after job: {}", sum);
		}
	}

	public static class SavePersonAnnotationJobExecution {
		@BeforeJob
		public void beforeJob(JobExecution jobExecution) {
			log.info("annotation before job");
		}

		@AfterJob
		public void afterJob(JobExecution jobExecution) {
			int sum = jobExecution.getStepExecutions().stream().mapToInt(StepExecution::getWriteCount).sum();
			log.info("annotation after job: {}", sum);
		}
	}
}
