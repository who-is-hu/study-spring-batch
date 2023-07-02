package com.example.studyspringbatch.part4;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface UserRepository extends JpaRepository<User, Long> {
	@Query("select min(u.id) from User u")
	long findMinId();

	@Query("select max(u.id) from User u")
	long findMaxId();
}
