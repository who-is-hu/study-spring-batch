package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ItemProcessorConf {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;

	public ItemProcessorConf(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean
	public Job itemProcessorJob() {
		return jobBuilderFactory.get("itemProcessorJob")
			.incrementer(new RunIdIncrementer())
			.start(this.itemProcessorStep())
			.build();
	}

	@Bean
	public Step itemProcessorStep() {
		return stepBuilderFactory.get("itemProcessorStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.processor(itemProcessor())
			.writer(itemWriter())
			.build();
	}

	private ItemWriter<? super Person> itemWriter() {
		return items -> {
			items.forEach(x -> log.info("person id: {}", x.getId()));
		};
	}

	private ItemProcessor<? super Person,? extends Person> itemProcessor() {
		return item -> {
			if(item.getId() % 2 == 0) {
				return item;
			}
			return null;
		};
	}

	private ItemReader<Person> itemReader(){
		return new CustomItemReader<>(getItems());
	}

	private List<Person> getItems() {
		ArrayList<Person> items = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			items.add(new Person(i+1, "testname", "age", "test addr"));
		}
		return items;
	}
}
