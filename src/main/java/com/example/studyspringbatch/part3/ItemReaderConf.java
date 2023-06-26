package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class ItemReaderConf {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;
	final private DataSource dataSource;
	final private EntityManagerFactory entityManagerFactory;

	public ItemReaderConf(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource, EntityManagerFactory entityManagerFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
		this.entityManagerFactory = entityManagerFactory;
	}

	@Bean
	public Job itemReaderJob() throws Exception {
		return jobBuilderFactory.get("itemReaderJob")
			.incrementer(new RunIdIncrementer())
			.start(this.itemReaderStep())
			.next(this.csvFileStep())
			.next(this.jdbcStep())
			.next(this.jpaStep())
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

	@Bean
	public Step csvFileStep() throws Exception {
		return stepBuilderFactory.get("csvFileStep")
			.<Person, Person>chunk(10)
			.reader(this.csvFileItemReader())
			.writer(itemWriter())
			.build();
	}

	@Bean
	public Step jdbcStep() throws Exception {
		return stepBuilderFactory.get("jdbcStep")
			.<Person, Person>chunk(10)
			.reader(this.jdbcCursorItemReader())
			.writer(itemWriter())
			.build();
	}

	@Bean
	public Step jpaStep() throws Exception {
		return stepBuilderFactory.get("jpaStep")
			.<Person, Person>chunk(10)
			.reader(this.jpaCursorItemReader())
			.writer(itemWriter())
			.build();
	}

	private JpaCursorItemReader<Person> jpaCursorItemReader() throws Exception {
		JpaCursorItemReader<Person> cursorItemReader = new JpaCursorItemReaderBuilder<Person>()
			.name("jpaItemReader")
			.entityManagerFactory(entityManagerFactory)
			.queryString("select p from Person p")
			.build();
		cursorItemReader.afterPropertiesSet();
		return cursorItemReader;
	}

	private JdbcCursorItemReader<Person> jdbcCursorItemReader() throws Exception {
		JdbcCursorItemReader<Person> itemReader = new JdbcCursorItemReaderBuilder<Person>()
			.name("jdbcItemReader")
			.dataSource(dataSource)
			.sql("select * from person")
			.rowMapper(((rs, rowNum) ->
				new Person(
					rs.getInt(1),
					rs.getString(2),
					rs.getString(3),
					rs.getString(4)
				)
			))
			.build();
		itemReader.afterPropertiesSet();
		return itemReader;
	}

	private FlatFileItemReader<Person> csvFileItemReader() throws Exception {
		DefaultLineMapper<Person> defaultLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames("id", "name", "age", "address");
		defaultLineMapper.setLineTokenizer(tokenizer);

		defaultLineMapper.setFieldSetMapper(fieldSet -> {
			int id = fieldSet.readInt("id");
			String name = fieldSet.readString("name");
			String age = fieldSet.readString("age");
			String address = fieldSet.readString("address");

			return new Person(id, name, age, address);
		});

		FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
			.name("csvFileItemReader")
			.encoding("UTF-8")
			.resource(new ClassPathResource("test.csv"))
			.linesToSkip(1)
			.lineMapper(defaultLineMapper)
			.build();
		itemReader.afterPropertiesSet();

		return itemReader;
	}

	private ItemWriter<Person> itemWriter() {
		return items -> log.info(items.stream().map(Person::getName).collect(Collectors.joining(" ,")));
	}

	private List<Person> getItems() {
		ArrayList<Person> items = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			items.add(new Person(i, "testname", "age", "test addr"));
		}
		return items;
	}
}
