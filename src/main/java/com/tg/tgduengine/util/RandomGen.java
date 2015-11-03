package com.tg.tgduengine.util;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Minutes;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomGen {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RandomGen.class);
	
	public static void main(String args[]){
		RandomGen rg = new RandomGen();
		LOGGER.info("Mobile: "+rg.getMobileNum());		
		LOGGER.info("Email: "+rg.getEmail());	
	}
	
	public long getMobileNum(){
		
		return (long) Math.floor(Math.random() * 9000000000L) + 1000000000L;		
		
	}
	
	public String getEmail(){
	
		long number = (long) Math.floor(Math.random() * 9000000000L) + 1000000000L;	
		return String.valueOf(number) + "@verixyz.com";		
		
	}
	
	
	public void getElpasedTime(){
		DateTime myBirthDate = new DateTime(1978, 3, 26, 12, 35, 0, 0);
		DateTime now = new DateTime();
		Period period = new Period(myBirthDate, now);

		PeriodFormatter formatter = new PeriodFormatterBuilder()
		    .appendSeconds().appendSuffix(" seconds ago\n")
		    .appendMinutes().appendSuffix(" minutes ago\n")
		    .appendHours().appendSuffix(" hours ago\n")
		    .appendDays().appendSuffix(" days ago\n")
		    .appendWeeks().appendSuffix(" weeks ago\n")
		    .appendMonths().appendSuffix(" months ago\n")
		    .appendYears().appendSuffix(" years ago\n")
		    .printZeroNever()
		    .toFormatter();

		String elapsed = formatter.print(period);
		LOGGER.info(elapsed);
	}
}

