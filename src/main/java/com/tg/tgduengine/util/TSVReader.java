package com.tg.tgduengine.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class TSVReader {

	public List<String[]> parseData(){
		TsvParserSettings settings = new TsvParserSettings();
		settings.selectIndexes(0, 1, 7); //0,1,7 -> Mobile Data Usage
		
	    //the file used in the example uses '\n' as the line separator sequence.
	    //the line separator sequence is defined here to ensure systems such as MacOS and Windows
	    //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
	    settings.getFormat().setLineSeparator("\n");

	    // creates a TSV parser
	    TsvParser parser = new TsvParser(settings);
	    List<String[]> allRows = null;
	    // parses all rows in one go.
	    try {
			allRows = parser.parseAll(getReader("C:\\Ravi\\SampleTSV.txt"));
				/*long millisecondsSinceEpoch = Long.parseLong(cpntent[1]);
				DateTime dt = new DateTime(millisecondsSinceEpoch);//YYY-MM-DD
				DateTime  dataisageEndTime = dt.plusMinutes(10);
				// and here's how to get the String representation
				//final String timeString2 = dt.toString("MM/dd/YYYY HH:mm:ss");
				
				System.out.println("Start Time: "+dt);
				System.out.println("End Time: "+dataisageEndTime);*/
				
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return allRows;
	}
	
	/*public void getDate(){
		
		long millisecondsSinceEpoch = Long.parseLong(cpntent[1]);
		
		
		DateTime dt = new DateTime(millisecondsSinceEpoch);//YYY-MM-DD
		DateTime  dataisageEndTime = dt.plusMinutes(10);
		// and here's how to get the String representation
		//final String timeString2 = dt.toString("MM/dd/YYYY HH:mm:ss");
		
		System.out.println("Start Time: "+dt);
		System.out.println("End Time: "+dataisageEndTime);
	}*/
	public Reader getReader(String relativePath)  {
        System.out.println("File: "+relativePath+"this calss: "+this.getClass());
        InputStreamReader isR = null;
        try {
        	isR = new InputStreamReader(new FileInputStream(relativePath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return isR;
        
    }


}


