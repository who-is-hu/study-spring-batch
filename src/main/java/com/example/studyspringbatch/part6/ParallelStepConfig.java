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
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.tasklet.TaskletStep;
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

/**
 * chunk 단위 멀티스레딩
 */
@Slf4j
@Configuration
public class ParallelStepConfig {
	private final int CHUNK = 1000;
	private final String JOB_NAME = "parallelJob";
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;
	final private UserRepository userRepository;
	final private EntityManagerFactory entityManagerFactory;
	final private DataSource dataSource;
	final private TaskExecutor taskExecutor;

	public ParallelStepConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, UserRepository userRepository, EntityManagerFactory entityManagerFactory, DataSource dataSource, TaskExecutor taskExecutor) {
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
			.start(this.saveUserFLow())
			.next(this.splitFlow(null))
			.build()
			.build();
	}

	@Bean(JOB_NAME+"userlevleupStep")
	public Step userLevelUpStep() throws Exception {
		return stepBuilderFactory.get(JOB_NAME + "userLevelUpStep")
			.<User, User>chunk(CHUNK)
			.reader(this.itemReader(null, null))
			.processor(this.itemProcessor())
			.writer(this.itemWriter())
			.build();
	}

	@Bean(JOB_NAME+"_taskhandler")
	PartitionHandler taskExecutorPartitionHandler() throws Exception {
		TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
		handler.setStep(userLevelUpStep());
		handler.setTaskExecutor(this.taskExecutor);
		handler.setGridSize(8);

		return handler;
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


	@Bean(JOB_NAME + "jpaPaging")
	@StepScope
	JpaPagingItemReader<? extends User> itemReader(
		@Value("#{stepExecutionContext[minId]}") Long minId,
		@Value("#{stepExecutionContext[maxId]}") Long maxId) throws Exception {

		Map<String, Object> params = new HashMap<>();
		params.put("minId", minId);
		params.put("maxId", maxId);
		JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
			.queryString("select u from User u where u.id between :minId and :maxId")
			.entityManagerFactory(entityManagerFactory)
			.pageSize(CHUNK)
			.name(JOB_NAME + "userItemReader")
			.parameterValues(params)
			.build();

		itemReader.afterPropertiesSet();

		return itemReader;
	}

	@Bean(JOB_NAME+"_splitFlow")
	@JobScope
	public Flow splitFlow(@Value("#{jobParameters[date]}") String date) throws Exception {
		Flow userLevelUpFlow = new FlowBuilder<SimpleFlow>(JOB_NAME+"_userLevelUpFlow")
			.start(userLevelUpStep())
			.build();

		// 이 두개 flow를 병렬로
		return new FlowBuilder<SimpleFlow>(JOB_NAME+"_splitFlow")
			.split(this.taskExecutor)
			.add(userLevelUpFlow, orderStatisticsFlow(date))
			.build();
	}

	@Bean(JOB_NAME+"_saveUserFLow")
	public Flow saveUserFLow() throws Exception {
		TaskletStep step = stepBuilderFactory.get(JOB_NAME + "saveUserStep")
			.tasklet(new SaveUserTasklet(userRepository))
			.build();

		return new FlowBuilder<SimpleFlow>(JOB_NAME+"_saveUserFLow")
			.start(step)
			.build();
	}

	public Flow orderStatisticsFlow(String date) throws Exception {
		return new FlowBuilder<SimpleFlow>(JOB_NAME+"_statisticsFlow")
			.start(new JobParametersDecide("date"))
			.on(JobParametersDecide.CONTINUE.getName())
			.to(this.orderStatisticsStep(date))
			.build();
	}

	public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
		return this.stepBuilderFactory.get(JOB_NAME + "orderStatisticsStep")
			.<OrderStatistics, OrderStatistics>chunk(CHUNK)
			.reader(orderStatisticsItemReader(date))
			.writer(orderStatisticsItemWriter(date))
			.build();
	}

	private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
		YearMonth yearMonth = YearMonth.parse(date);
		String fileName = yearMonth.getYear() + "_" + yearMonth.getMonthValue() + "_일별_주문_금액.csv";

		BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[]{"amount", "date"});

		DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(fieldExtractor);

		FlatFileItemWriter<OrderStatistics> orderStatisticsItemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
			.resource(new FileSystemResource("output/" + fileName))
			.lineAggregator(lineAggregator)
			.name(JOB_NAME + "orderStatisticsItemWriter")
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
			.name(JOB_NAME + "orderStatisticsItemReader")
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
