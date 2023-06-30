package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import java.util.function.Consumer;

@Configuration
@Slf4j
public class CsvToDbConfig {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;
	final private EntityManagerFactory entityManagerFactory;

	public CsvToDbConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.entityManagerFactory = entityManagerFactory;
	}

	@Bean
	public Job csvToDbJob() throws Exception {
		return jobBuilderFactory.get("csvToDbJob")
			.incrementer(new RunIdIncrementer())
			.start(this.csvToDbStep(null))
			.listener(new SavePersonListener.SavePersonJobExecutionListener())
			.listener(new SavePersonListener.SavePersonAnnotationJobExecution())
			.build();
	}

	@Bean
	@JobScope
	public Step csvToDbStep(@Value("#{jobParameters[allowDuplicate]}") String allowDuplicate) throws Exception {
		System.out.println(allowDuplicate);
		return stepBuilderFactory.get("csvToDbStep")
			.<Person, Person>chunk(10)
			.reader(csvFileItemReader())
			.processor(new DuplicateValidationProcessor<Person>(Person::getName, Boolean.parseBoolean(allowDuplicate)))
			.writer(itemWriter())
			.listener(new SavePersonListener.SavePersonStepExecutionListener())
			.build();
	}

	private ItemWriter<? super Person> itemWriter() throws Exception {
		JpaItemWriter<Person> jpaWriter = new JpaItemWriterBuilder<Person>()
			.entityManagerFactory(entityManagerFactory)
			.build();

		ItemWriter<Person> logWriter = items -> {
			log.info("item size: {}", items.size());
		};

		CompositeItemWriter<Person> compositeItemWriter = new CompositeItemWriterBuilder<Person>()
			.delegates(jpaWriter, logWriter) // 순서대로
			.build();

		compositeItemWriter.afterPropertiesSet();

		return compositeItemWriter;
	}

	private FlatFileItemReader<Person> csvFileItemReader() throws Exception {
		DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames("name", "age", "address");
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fieldSet -> {
			String name = fieldSet.readString(0);
			String age = fieldSet.readString(1);
			String address = fieldSet.readString(2);
			return new Person(name, age, address);
		});

		FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
			.name("csvFileItemReader")
			.encoding("UTF-8")
			.resource(new ClassPathResource("persons.csv"))
			.linesToSkip(1)
			.lineMapper(lineMapper)
			.build();
		itemReader.afterPropertiesSet();

		return itemReader;
	}
}
