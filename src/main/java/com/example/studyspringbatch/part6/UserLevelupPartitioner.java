package com.example.studyspringbatch.part6;

import com.example.studyspringbatch.part4.UserRepository;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class UserLevelupPartitioner implements Partitioner {
	private final UserRepository userRepository;

	public UserLevelupPartitioner(UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		//grid size  = 스레드갯수
		long minId = userRepository.findMinId();
		long maxId = userRepository.findMaxId();

		long targetSize = (maxId-minId) / gridSize + 1;

		/**
		 * partition0 : 1~5000
		 * partition1 : 5001~10000
		 *
		 */
		HashMap<String, ExecutionContext> result = new HashMap<String, ExecutionContext>();

		long number = 0;
		long start = minId;
		long end = start + targetSize - 1;
		while(start <= maxId){
			ExecutionContext executionContext = new ExecutionContext();
			result.put("partition"+number, executionContext);

			if(end >= maxId){
				end = maxId;
			}
			executionContext.putLong("minId", start);
			executionContext.putLong("maxId", end);

			start += targetSize;
			end += targetSize;
			number++;
		}
		return result;
	}
}
