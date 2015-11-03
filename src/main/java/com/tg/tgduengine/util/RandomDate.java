package com.tg.tgduengine.util;

import java.sql.Timestamp;

import org.joda.time.DateTime;

public class RandomDate {

	public static DateTime getRandomCallTime(String dateString){
		
		String sampleStart = dateString.concat(" 00:00:00");
		String sampleEnd = dateString.concat(" 23:45:00");
		long offset = Timestamp.valueOf(sampleStart).getTime();
		long end = Timestamp.valueOf(sampleEnd).getTime();
		long diff = end - offset + 1;
		Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
		return new DateTime(rand);
		
	}

}
