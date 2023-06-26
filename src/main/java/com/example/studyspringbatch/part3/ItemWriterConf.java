package com.example.studyspringbatch.part3;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class ItemWriterConf {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;
	final private DataSource dataSource;
	final private EntityManagerFactory entityManagerFactory;

	public ItemWriterConf(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource, EntityManagerFactory entityManagerFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
		this.entityManagerFactory = entityManagerFactory;
	}

	@Bean
	public Job itemWriterJob() throws Exception {
		return jobBuilderFactory.get("itemWriterJob")
			.incrementer(new RunIdIncrementer())
			.start(this.csvItemWriterStep())
//			.next(this.jdbcBatchItemWriterStep())
			.next(this.jpaItemWriterStep())
			.build();
	}

	@Bean
	public Step csvItemWriterStep() throws Exception {
		return stepBuilderFactory.get("csvItemWriterStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
//			.processor()
			.writer(csvItemWriter())
			.build();
	}

	private ItemWriter<? super Person> csvItemWriter() throws Exception {
		BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] {"id", "name", "age", "address"});
		DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(fieldExtractor);

		FlatFileItemWriter<Person> writer = new FlatFileItemWriterBuilder<Person>()
			.name("csvFileItemWriter")
			.encoding("UTF-8")
			.resource(new FileSystemResource("output/test-output.csv"))
			.lineAggregator(lineAggregator)
			.headerCallback(writer1 -> writer1.write("id,이름,나이,주소"))
			.footerCallback(writer1 -> writer1.write("-------------------------\n"))
			.append(true)
			.build();

		writer.afterPropertiesSet();

		return writer;
	}

	@Bean
	public Step jdbcBatchItemWriterStep() {
		return this.stepBuilderFactory.get("jdbcBatchItemWriter")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.writer(jdbcBatchItemWriter())
			.build();
	}

	@Bean
	public ItemWriter<? super Person> jdbcBatchItemWriter() {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriterBuilder<Person>()
			.dataSource(dataSource)
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql("insert into person(name, age, address) values(:name, :age, :address)")
			.build();

		writer.afterPropertiesSet();

		return writer;
	}

	@Bean
	public Step jpaItemWriterStep() throws Exception {
		return stepBuilderFactory.get("jpaItemWriterStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.writer(jpaItemWriter())
			.build();
	}

	private ItemWriter<Person> jpaItemWriter() throws Exception {
		JpaItemWriter<Person> writer = new JpaItemWriterBuilder<Person>()
			.entityManagerFactory(entityManagerFactory)
			.usePersist(true)
			.build();
		writer.afterPropertiesSet();
		return writer;
	}

	private ItemReader<Person> itemReader(){
		return new CustomItemReader<>(getItems());
	}

	private List<Person> getItems(){
		List<Person> items = new ArrayList<>();
		for(int i=0; i<100; i++){
			items.add(new Person("test name" + i, "age", "test adr" + i));
		}

		return items;
	}
}
