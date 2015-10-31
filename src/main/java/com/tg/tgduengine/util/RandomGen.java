package com.tg.tgduengine.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

public class RandomGen {
	public static void main(String args[]){
		RandomGen rg = new RandomGen();
		System.out.println("Mobile: "+rg.getMobileNum());		
		System.out.println("Email: "+rg.getEmail());	
		rg.duration();
		rg.getElpasedTime();
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
	
	public void duration(){
		DateTime today = new DateTime();
		DateTime yesterday = today.minusDays(1);

		Duration duration = new Duration(yesterday, today);
		System.out.println(duration.getStandardDays());
		System.out.println(duration.getStandardHours());
		System.out.println(duration.getStandardMinutes());
		System.out.println(Minutes.minutesBetween(yesterday, today).getMinutes());
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
		System.out.println(elapsed);
	}
}

