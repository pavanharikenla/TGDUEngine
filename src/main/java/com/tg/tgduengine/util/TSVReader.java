package com.tg.tgduengine.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class TSVReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSVReader.class);
	
	public List<String[]> parseData(){
		TsvParserSettings settings = new TsvParserSettings();
		settings.selectIndexes(0, 1, 7);
	    //the file used in the example uses '\n' as the line separator sequence.
	    //the line separator sequence is defined here to ensure systems such as MacOS and Windows
	    //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
	    settings.getFormat().setLineSeparator("\n");

	    // creates a TSV parser
	    TsvParser parser = new TsvParser(settings);
	    List<String[]> allRows = null;
	    // parses all rows in one go.
	    try {
			allRows = parser.parseAll(getReader("C:\\Ravi\\Part_1.txt"));
			} catch (Exception e) {
				LOGGER.error("Exception in parsing the file: ",e);
		}
	    
	    return allRows;
	}
	
	public List<String[]> parseCallData(){
		TsvParserSettings settings = new TsvParserSettings();
		settings.selectIndexes(0, 1, 5, 6);
	    //the file used in the example uses '\n' as the line separator sequence.
	    //the line separator sequence is defined here to ensure systems such as MacOS and Windows
	    //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
	    settings.getFormat().setLineSeparator("\n");

	    // creates a TSV parser
	    TsvParser parser = new TsvParser(settings);
	    List<String[]> allRows = null;
	    // parses all rows in one go.
	    try {
			allRows = parser.parseAll(getReader("E:\\Personal\\sample\\sample.txt"));

		} catch (Exception e) {
			
			LOGGER.error("Exception in parsing the file: ",e);
		}
	    
	    return allRows;
	}
	
	
public Reader getReader(String relativePath)  {
        
        InputStreamReader isR = null;
        try {
        	isR = new InputStreamReader(new FileInputStream(relativePath));
		} catch (FileNotFoundException e) {
			LOGGER.error("Exception in parsing the file: " + relativePath, e);
	        
		}
        
        
        return isR;
        
    }

}


