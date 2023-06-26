package com.example.studyspringbatch.part3;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Getter
@AllArgsConstructor
@Entity
@NoArgsConstructor
public class Person {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int id;
	private String name;
	private String age;
	private String address;

	public Person(String name, String age, String address) {
		this.name = name;
		this.age = age;
		this.address = address;
	}
}
