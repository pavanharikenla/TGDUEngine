package com.tg.tgduengine.util;

public class RandomGen {
	public static void main(String args[]){
		RandomGen rg = new RandomGen();
		rg.mobileNum();		
	}
	
	public long mobileNum(){
		long number = 0;		
		number = (long) Math.floor(Math.random() * 9000000000L) + 1000000000L;		
		System.out.println(number);
		return number;
	}
}
