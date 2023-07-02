package com.example.studyspringbatch.part4;

import com.example.studyspringbatch.part5.Orders;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SaveUserTasklet implements Tasklet {

	private final UserRepository userRepository;
	private final int SIZE = 100;

	public SaveUserTasklet(UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		List<User> users = createUsers();
		Collections.shuffle(users);
		userRepository.saveAll(users);

		return RepeatStatus.FINISHED;
	}

	private List<User> createUsers() {
		ArrayList<User> users = new ArrayList();

		for (int i = 0; i < SIZE; i++) {
			users.add(User.builder()
				.orders(Collections.singletonList(
					Orders.builder()
						.amount(1000)
						.createdDate(LocalDate.of(2023, 7, 1))
						.itemName("item" + i)
						.build()
				))
				.name("test name" + i)
				.level(Level.NORMAL)
				.build());
		}

		for (int i = 0; i < SIZE; i++) {
			users.add(User.builder()
				.orders(Collections.singletonList(
					Orders.builder()
						.amount(200000)
						.createdDate(LocalDate.of(2023, 7, 2))
						.itemName("item" + i)
						.build()
				))
				.name("test name" + i)
				.level(Level.NORMAL)
				.build());
		}

		for (int i = 0; i < SIZE; i++) {
			users.add(User.builder()
				.orders(Collections.singletonList(
					Orders.builder()
						.amount(300000)
						.createdDate(LocalDate.of(2023, 7, 3))
						.itemName("item" + i)
						.build()
				))
				.name("test name" + i)
				.level(Level.NORMAL)
				.build());
		}

		for (int i = 0; i < SIZE; i++) {
			users.add(User.builder()
				.orders(Collections.singletonList(
					Orders.builder()
						.amount(500000)
						.createdDate(LocalDate.of(2023, 7, 4))
						.itemName("item" + i)
						.build()
				))
				.name("test name" + i)
				.level(Level.NORMAL)
				.build());
		}
		return users;
	}


}
