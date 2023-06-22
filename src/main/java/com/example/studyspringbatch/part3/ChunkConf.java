package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ChunkConf {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;

	public ChunkConf(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean
	public Job chunkJob(){
		return jobBuilderFactory.get("chunkJob")
			.incrementer(new RunIdIncrementer())
			.start(this.taskStep())
			.next(this.chunkStep(null))
			.build();
	}

	@Bean
	public Step taskStep(){
		return stepBuilderFactory.get("taskbase")
			.tasklet(this.tasklet())
			.build();
	}

	@Bean
	@JobScope
	public Step chunkStep(@Value("#{jobParameters[chunkSize]}") String chunkSize){
		return stepBuilderFactory.get("chunkStep")
			.<String, String>chunk(StringUtils.hasLength(chunkSize) ? Integer.parseInt(chunkSize) : 10)
			.reader(itemReader())
			.processor(itemProcessor())
			.writer(itemWriter())
			.build();
	}

	private ItemWriter<? super String> itemWriter() {
		return item -> log.info("chunk item size {}",item.size());
//		return item -> item.forEach(log::info);
	}

	private ItemProcessor<String, String> itemProcessor() {
		return item -> item + " processed";
	}

	private ItemReader<String> itemReader() {
		return new ListItemReader<>(getItems());
	}

	private Tasklet tasklet() {
		return (contribution, chunkContext) -> {
			StepExecution stepExecution = contribution.getStepExecution();
//			JobParameters jobParameters = stepExecution.getJobParameters();
			List<String> items = getItems();
			log.info("task size {}",items.size());

			return RepeatStatus.FINISHED;
		};
	}

	private List<String> getItems(){
		ArrayList<String> list = new ArrayList<>();
		for(int i=0; i<100; i++){
			list.add(i + " hello");
		}
		return list;
	}
}
