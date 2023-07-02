package com.example.studyspringbatch.part4;

import lombok.*;

import javax.persistence.*;
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

	@Column(name = "purchase_amount")
	private Integer purchaseAmount;

	public boolean availableLevelUp() {
		return Level.availableLevelUp(this.level, this.purchaseAmount);
	}

	public Level levelUp(){
		Level nextLevel = Level.getNextLevel(this.purchaseAmount);

		this.level = nextLevel;

		return nextLevel;
	}
}
