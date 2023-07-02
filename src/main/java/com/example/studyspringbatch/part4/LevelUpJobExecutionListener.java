package com.example.studyspringbatch.part4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.util.List;

@Slf4j
public class LevelUpJobExecutionListener implements JobExecutionListener {
	private final UserRepository userRepository;

	public LevelUpJobExecutionListener(UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {

	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		List<User> all = userRepository.findAll();

		long time = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();

		log.info("user levelUp bach");
		log.info("=========================");
		log.info("total: {}, take: {}ms", all.size(), time);
	}
}
