package com.example.studyspringbatch.part6;

import com.example.studyspringbatch.part4.LevelUpJobExecutionListener;
import com.example.studyspringbatch.part4.SaveUserTasklet;
import com.example.studyspringbatch.part4.User;
import com.example.studyspringbatch.part4.UserRepository;
import com.example.studyspringbatch.part5.JobParametersDecide;
import com.example.studyspringbatch.part5.OrderStatistics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * chunk 단위 멀티스레딩
 */
@Slf4j
@Configuration
public class MultiThreadUserJob {
	private final int CHUNK = 1000;
	private final String JOB_NAME = "mutliThreadUserJob";
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;
	final private UserRepository userRepository;
	final private EntityManagerFactory entityManagerFactory;
	final private DataSource dataSource;
	final private TaskExecutor taskExecutor;

	public MultiThreadUserJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, UserRepository userRepository, EntityManagerFactory entityManagerFactory, DataSource dataSource, TaskExecutor taskExecutor) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.userRepository = userRepository;
		this.entityManagerFactory = entityManagerFactory;
		this.dataSource = dataSource;
		this.taskExecutor = taskExecutor;
	}

	@Bean(JOB_NAME)
	public Job userGradeUpgradeJob() throws Exception {
		return jobBuilderFactory.get(JOB_NAME)
			.incrementer(new RunIdIncrementer())
			.start(this.saveUserStep())
			.next(this.userLevelUpStep())
//			.next(this.orderStatisticsStep(null))
			.listener(new LevelUpJobExecutionListener(userRepository))
			.next(new JobParametersDecide("date"))
			.on(JobParametersDecide.CONTINUE.getName())
			.to(this.orderStatisticsStep(null))
			.build()
			.build();
	}

	@Bean(JOB_NAME+"userlevleupStep")
	public Step userLevelUpStep() throws Exception {
		return stepBuilderFactory.get(JOB_NAME+"userLevelUpStep")
			.<User, User>chunk(CHUNK)
			.reader(this.itemReader())
			.processor(this.itemProcessor())
			.writer(this.itemWriter())
			.taskExecutor(this.taskExecutor)
			.throttleLimit(8) // 몇개의 스레드로 청크를 처리할지
			.build();
	}

	private ItemWriter<User> itemWriter() {
		ItemWriter<User> itemWriter = users -> {
			users.forEach(u -> {
				u.levelUp();
				userRepository.save(u);
			});
		};
		return itemWriter;
	}

	private ItemProcessor<User, User> itemProcessor() {
		ItemProcessor<User, User> itemProcessor = user -> {
			if (user.availableLevelUp()) {
				return user;
			}
			return null;
		};

		return itemProcessor;
	}


	private ItemReader<? extends User> itemReader() throws Exception {
		JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
			.queryString("select u from User u")
			.entityManagerFactory(entityManagerFactory)
			.pageSize(CHUNK)
			.name(JOB_NAME+"userItemReader")
			.build();

		itemReader.afterPropertiesSet();

		return itemReader;
	}

	@Bean(JOB_NAME+"svaeUserSTep")
	public Step saveUserStep() throws Exception {
		return stepBuilderFactory.get(JOB_NAME+"saveUserStep")
			.tasklet(new SaveUserTasklet(userRepository))
			.build();
	}

	@Bean(JOB_NAME+"orderstatistics")
	@JobScope
	public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
		return this.stepBuilderFactory.get(JOB_NAME+"orderStatisticsStep")
			.<OrderStatistics, OrderStatistics>chunk(CHUNK)
			.reader(orderStatisticsItemReader(date))
			.writer(orderStatisticsItemWriter(date))
			.build();
	}

	private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
		YearMonth yearMonth = YearMonth.parse(date);
		String fileName = yearMonth.getYear() + "_" + yearMonth.getMonthValue() + "_일별_주문_금액.csv";

		BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] {"amount", "date"});

		DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(fieldExtractor);

		FlatFileItemWriter<OrderStatistics> orderStatisticsItemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
			.resource(new FileSystemResource("output/" + fileName))
			.lineAggregator(lineAggregator)
			.name(JOB_NAME+"orderStatisticsItemWriter")
			.encoding("UTF-8")
			.headerCallback(writer -> writer.write("total_amount, date"))
			.build();

		orderStatisticsItemWriter.afterPropertiesSet();

		return orderStatisticsItemWriter;
	}

	private ItemReader<OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
		YearMonth yearMonth = YearMonth.parse(date);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("startDate", yearMonth.atDay(1));
		parameters.put("endDate", yearMonth.atEndOfMonth());

		Map<String, Order> sortKey = new HashMap<>();
		sortKey.put("created_date", Order.ASCENDING);

		JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
			.dataSource(dataSource)
			.rowMapper((rs, rowNum) -> OrderStatistics.builder()
				.amount(rs.getString(1))
				.date(LocalDate.parse(rs.getString(2), DateTimeFormatter.ISO_DATE))
				.build())
			.pageSize(CHUNK)
			.name(JOB_NAME+"orderStatisticsItemReader")
			.selectClause("sum(amount), created_date")
			.fromClause("orders")
			.whereClause("created_date between :startDate and :endDate")
			.groupClause("created_date")
			.parameterValues(parameters)
			.sortKeys(sortKey)
			.build();

		itemReader.afterPropertiesSet();
		return itemReader;
	}

}
