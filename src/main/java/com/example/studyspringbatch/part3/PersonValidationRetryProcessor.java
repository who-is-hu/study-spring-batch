package com.example.studyspringbatch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

@Slf4j
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {

	private RetryTemplate retryTemplate;

	public PersonValidationRetryProcessor() {
		this.retryTemplate = new RetryTemplateBuilder()
			.maxAttempts(3)
			.retryOn(NotFoundNameException.class)
			.withListener(new SavePersonRetryListener())
			.build();
	}

	@Override
	public Person process(Person item) throws Exception {
		return this.retryTemplate.execute(context -> {
			// retry callback
			if(item.isNotEmpty()){
				return item;
			}
			throw new NotFoundNameException();
		}, context -> {
			//recovery callback
			return item.unknownName();
		});
	}

	public static class SavePersonRetryListener implements RetryListener {
		@Override
		public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
			// retry를 시작하는 설정
			// true여야 template의 retry callback 호출
			return true;
		}

		@Override
		public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
			//retry 종료후에
			log.info("retry close");
		}

		@Override
		public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
			//설정한 exception 터지면 발생
			log.info("on error");
		}
	}
}
