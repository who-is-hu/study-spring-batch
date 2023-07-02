package com.example.studyspringbatch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.swing.*;
import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication
@EnableBatchProcessing
public class StudySpringBatchApplication {

	public static void main(String[] args) {
		System.exit(
			SpringApplication.exit(
				SpringApplication.run(StudySpringBatchApplication.class, args)
			)
		);
	}

	@Bean
	@Primary
	TaskExecutor taskExecutor(){
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(10);
		taskExecutor.setMaxPoolSize(20);
		taskExecutor.setThreadNamePrefix("batch-thread-");
		taskExecutor.initialize();
		return taskExecutor;
	}

}
