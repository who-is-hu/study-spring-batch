package com.example.studyspringbatch.part4;

import com.example.studyspringbatch.part5.Orders;
import lombok.*;

import javax.persistence.*;
import java.util.List;
import java.util.Objects;

enum Level {
	VIP(500000, null),
	GOLD(500000, VIP),
	SILVER(300000, GOLD),
	NORMAL(200_000, SILVER),
	;
	int nextAmount;
	Level nextLevel;

	Level(int purchaseAmount, Level nextLevel) {
		this.nextLevel = nextLevel;
		this.nextAmount = purchaseAmount;
	}

	public static boolean availableLevelUp(Level level, Integer purchaseAmount) {
		if(Objects.isNull(level)){
			return false;
		}
		if(Objects.isNull(level.nextLevel)){
			return false;
		}
		return purchaseAmount >= level.nextAmount;
	}

	public static Level getNextLevel(Integer purchaseAmount) {
		if(purchaseAmount >= Level.VIP.nextAmount){
			return VIP;
		}
		if(purchaseAmount >= Level.GOLD.nextAmount){
			return GOLD.nextLevel;
		}
		if(purchaseAmount >= Level.SILVER.nextAmount){
			return SILVER.nextLevel;
		}
		if(purchaseAmount >= Level.NORMAL.nextAmount){
			return NORMAL.nextLevel;
		}
		return NORMAL;
	}

	Boolean isSatisfyingCondition(Long amount){
		return amount >= this.nextAmount;
	}
}

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class User {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(length = 100)
	private String name;

	@Enumerated(EnumType.STRING)
	private Level level = Level.NORMAL;

	@OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
	@JoinColumn(name = "user_id")
	List<Orders> orders;

	private int getTotalAmount(){
		 return this.orders.stream()
			 .mapToInt(Orders::getAmount)
			 .sum();
	}

	public boolean availableLevelUp() {
		return Level.availableLevelUp(this.level, this.getTotalAmount());
	}

	public Level levelUp(){
		Level nextLevel = Level.getNextLevel(this.getTotalAmount());

		this.level = nextLevel;

		return nextLevel;
	}
}
