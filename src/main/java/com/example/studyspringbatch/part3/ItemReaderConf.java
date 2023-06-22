package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class ItemReaderConf {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;

	public ItemReaderConf(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean
	public Job itemReaderJob() {
		return jobBuilderFactory.get("itemReaderJob")
			.incrementer(new RunIdIncrementer())
			.start(this.itemReaderStep())
			.build();
	}

	@Bean
	public Step itemReaderStep() {
		return stepBuilderFactory.get("itemReaderStep")
			.<Person, Person>chunk(10)
			.reader(new CustomItemReader<>(getItems()))
			.writer(itemWriter())
			.build();
	}

	private ItemWriter<Person> itemWriter() {
		return items -> log.info(items.stream().map(Person::getName).collect(Collectors.joining(" ,")));
	}

	private List<Person> getItems() {
		ArrayList<Person> items = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			items.add(new Person(i, "testname", 12, "test addr"));
		}
		return items;
	}
}
