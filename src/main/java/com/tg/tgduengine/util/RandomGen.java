package com.tg.tgduengine.util;

public class RandomGen {
	public static void main(String args[]){
		RandomGen rg = new RandomGen();
		System.out.println("Mobile: "+rg.getMobileNum());		
		System.out.println("Email: "+rg.getEmail());				
	}
	
	public long getMobileNum(){
		long number = 0;		
		number = (long) Math.floor(Math.random() * 9000000000L) + 1000000000L;		
		return number;
	}
	
	public String getEmail(){
		String email = "default@verixyz.com";		
		long number = (long) Math.floor(Math.random() * 9000000000L) + 1000000000L;	
		email = String.valueOf(number) + "@verixyz.com";		
		return email;
	}
}

