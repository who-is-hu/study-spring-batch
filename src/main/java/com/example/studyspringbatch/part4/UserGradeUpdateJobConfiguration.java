package com.example.studyspringbatch.part4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Slf4j
@Configuration
public class UserGradeUpdateJobConfiguration {
	final private JobBuilderFactory jobBuilderFactory;
	final private StepBuilderFactory stepBuilderFactory;
	final private UserRepository userRepository;
	final private EntityManagerFactory entityManagerFactory;

	public UserGradeUpdateJobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, UserRepository userRepository, EntityManagerFactory entityManagerFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.userRepository = userRepository;
		this.entityManagerFactory = entityManagerFactory;
	}

	@Bean
	public Job userGradeUpgradeJob() throws Exception {
		return jobBuilderFactory.get("userGradeUpgradeJob")
			.incrementer(new RunIdIncrementer())
			.start(this.saveUserStep())
			.next(this.userLevelUpStep())
			.listener(new LevelUpJobExecutionListener(userRepository))
			.build();
	}

	@Bean
	public Step userLevelUpStep() throws Exception {
		return stepBuilderFactory.get("userLevelUpStep")
			.<User, User>chunk(100 )
			.reader(this.itemReader())
			.processor(this.itemProcessor())
			.writer(this.itemWriter())
			.build();
	}

	private ItemWriter<? super User> itemWriter() {
		return users -> {
			users.forEach(u -> {
				u.levelUp();
				userRepository.save(u);
			});
		};
	}

	private ItemProcessor<? super User, ? extends User> itemProcessor() {
		return user -> {
			if (user.availableLevelUp()){
				return user;
			}
			return null;
		};
	}

	private ItemReader<? extends User> itemReader() throws Exception {
		JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
			.queryString("select u from User u")
			.entityManagerFactory(entityManagerFactory)
			.pageSize(100)
			.name("userItemReader")
			.build();

		itemReader.afterPropertiesSet();

		return itemReader;
	}

	@Bean
	public Step saveUserStep() throws Exception {
		return stepBuilderFactory.get("saveUserStep")
			.tasklet(new SaveUserTasklet(userRepository))
			.build();
	}

}
