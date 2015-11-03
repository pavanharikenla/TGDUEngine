package com.tg.tgduengine.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tg.tgduengine.util.RandomDate;
import com.tg.tgduengine.util.RandomGen;
import com.tg.tgduengine.util.TSVReader; 

public class SolrIndexer {

   
	private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexer.class);
	static String line;
	static final String url = "http://ec2-52-32-54-95.us-west-2.compute.amazonaws.com:8983/solr/collection1"; //Solr Instance
	SolrServer server ;
    static final List<String> location;
    static final List<String> calledPartyNetwork;
    static final List<String> cDRTypeList;
    static final List<String> appList;
    
    static {
    	  location = new ArrayList<String>(11);
    	  location.add("Newark,New Jersey");
    	  location.add("San Francisco,California");
    	  location.add("San Antonio,Texas");
    	  location.add("Chicago,Illinois");
    	  location.add("Jacksonville,Florida");
    	  
    	  location.add("New York City,New York");
    	  location.add("Columbus,Ohio");
    	  location.add("Oklahoma City,Oklahoma");
    	  location.add("Austin,Texas");
    	  location.add("Charlotte,North Carolina");
    	  location.add("Baskin Ridge,Texas");
    	  
    	  calledPartyNetwork = new ArrayList<String>(6);
    	  calledPartyNetwork.add("AT&T");
    	  calledPartyNetwork.add("Verizon");
    	  calledPartyNetwork.add("Lyca");
    	  calledPartyNetwork.add("T-Mobile");
    	  calledPartyNetwork.add("Sprint");
    	  calledPartyNetwork.add("CenturyLink");
    	  
    	  cDRTypeList = new ArrayList<String>(3);
    	  cDRTypeList.add("Data");
    	  cDRTypeList.add("Call");
    	  cDRTypeList.add("SMS");
    	  
    	  appList = new ArrayList<String>(10);
    	  appList.add("Facebook");
    	  appList.add("Twitter");
    	  appList.add("WhatsApp");
    	  appList.add("Netflix");
    	  appList.add("Google Search");
    	  
    	  appList.add("Yahoo Stocks");
    	  appList.add("Pandora Radio");
    	  appList.add("Gmail");
    	  appList.add("Google Play");
    	  
   		  
    }
  
    
    
   /**
    * Method to index the CDR from a flat file
    * @throws FileNotFoundException
    * @throws IOException
    * @throws SolrServerException
    */
	public void indexCDRData(int cdrDataType) throws IOException, SolrServerException {

		TSVReader tsvReader = new TSVReader();
		List<String[]> parsedDataList = null;
		if(cdrDataType == 1)
			parsedDataList = tsvReader.parseData();
		else if(cdrDataType == 2)
			parsedDataList = tsvReader.parseCallData();
		server = new HttpSolrServer(url);
		RandomGen randomGen = new RandomGen();
		
		
		String[] cdrArrary;
		int cdrType = cdrDataType; //Solr Filed 1
		//String circleId = ""; //Solr Field 2
		
		
		/**
		 * Sample Date Value is: 2013-12-31T04:40:00.000+05:30
		 * So the data type in solar should be datetime (TrieDateField )
		 */
		DateTime dataUsageStartTime; //Solr Field 3 
		DateTime dataUsageEndtTime; //Solr Field 4 
		
		
		int dataUused = 1; //Solr Field 5. by default the data used is set to 1 MB
		int age; //Solr Field 6
		String ageGroup; //Solr Field 7
		String mobile; //Solr Field 8
		
		String locationString; 
		String[] splitLocation;
		String[] dataUsedSplit; 
		
		String city; //Solr Field 9
		String state; //Solr Field 10
		String email; //Solr Field 11
		 //Solr Filed 12
		
		
		long recepientMobile;// = 1234567890; //Solr Field 13
		DateTime callStart;// = new DateTime(); //Solr Filed 14
		DateTime callEnd;// = new DateTime(); //Solr Filed 15
		float callDuartion;// = 0.0F;// //Solr Filed 16
		float callCost = 0.0F; //Solr Filed 17
		String recipientNnetwork;// = ""; //Solr Filed 18
		
		
		
		for(int i=0; i<parsedDataList.size(); i++){
			SolrInputDocument cdr = new SolrInputDocument();
			
			//Fetch Parsed Data
			cdrArrary = parsedDataList.get(i);
			
			//Common Fields
			String circleId = cdrArrary[0];
			age = generateAge();
			ageGroup = getAgeGroup(age);
			mobile = getMobileNumber(i);
			locationString = generateUserLocation();
			splitLocation = locationString.split(",");
			city = splitLocation[0];
			state =  splitLocation[1];
			email = randomGen.getEmail();
			
			if(cdrType == 1)
				cdr.addField("id", i);
			else if(cdrType == 2)
				cdr.addField("id", i+2000000000);
			
			cdr.addField("circle_id", circleId);
			cdr.addField("age", age);
			cdr.addField("agegroup", ageGroup);
			cdr.addField("mobile", mobile);
			cdr.addField("city", city);
			cdr.addField("state", state);
			cdr.addField("email", email);
			
			
			//Fields specific to Usage
			if (cdrDataType == 1) { 
				
				if(cdrArrary[2] != null) {
					String dataUsedString = cdrArrary[2];
					dataUsedSplit = dataUsedString.split("\\.");
					dataUused = Integer.parseInt(dataUsedSplit[0]);

				}
				
				//Static Data Calculation
				String sampleDate = "";
				if(i < 13233)
					sampleDate = "2015-11-03";
				else if (i >= 13233 && i < 25999)
					sampleDate = "2015-11-02";
				else if (i >= 25999 && i < 29134)
					sampleDate = "2015-11-01";
				else if (i >= 29134 && i < 33447)
					sampleDate = "2015-10-31";
				else if (i >= 33447 && i < 39934)
					sampleDate = "2015-10-30";
				else if (i >= 39934 && i < 44000)
					sampleDate = "2015-10-29";
				else if (i >= 44000 && i < 100000)
					sampleDate = "2015-10-28";
				
				dataUsageStartTime = RandomDate.getRandomCallTime(sampleDate);
				dataUsageEndtTime = dataUsageStartTime.plusMinutes(dataUused);
				String mobileAppName = getAppName();
				
				cdr.addField("data_usage_start_time", dataUsageEndtTime.toDate());
				cdr.addField("data_usage_end_time", dataUsageEndtTime.toDate());
				cdr.addField("dataUsed", dataUused);
				cdr.addField("mobile_app_name", mobileAppName);
		
			} else if (cdrDataType == 2){ //This is for call
				
				recepientMobile = randomGen.getMobileNum();
				//Fetch Call Start
				String sampleDate = "";
				if(i < 100)
					sampleDate = "2015-11-03";
				else if (i >= 100 && i < 200)
					sampleDate = "2015-11-02";
				else if (i >= 200 && i < 300)
					sampleDate = "2015-11-01";
				else if (i >= 300 && i < 400)
					sampleDate = "2015-10-31";
				else if (i >= 400 && i < 500)
					sampleDate = "2015-10-30";
				else if (i >= 500 && i < 600)
					sampleDate = "2015-10-29";
				else if (i >= 600 && i < 100000)
					sampleDate = "2015-10-28";
				
				
				int callStartTime = 1;
				if(cdrArrary[2] != null) {
					String dataUsedString = cdrArrary[2];
					dataUsedSplit = dataUsedString.split("\\.");
					callStartTime = Integer.parseInt(dataUsedSplit[0]);

				}
				
				callStart = RandomDate.getRandomCallTime(sampleDate);
				callStart = callStart.plusHours(callStartTime);
				//Fetch Call End
				int callEndTime = 2;
				if(cdrArrary[3] != null) {
					String dataUsedString = cdrArrary[3];
					dataUsedSplit = dataUsedString.split("\\.");
					callEndTime = Integer.parseInt(dataUsedSplit[0]);

				}
				callEnd = callStart.plusHours(callEndTime);
				//Fetch Call Duration
				callDuartion = Minutes.minutesBetween(callStart, callEnd).getMinutes();
				if(callDuartion == 0){
					callDuartion = 60;
					callEnd = callStart.plusHours(1);
				}
				//Fetch Call Cost
				//Fetch recipient_network using 
				recipientNnetwork = getReceipientNetwork();
				cdr.addField("cat", "call");
				cdr.addField("recepient_mobile", recepientMobile);
				cdr.addField("call_start", callStart.toDate());
				cdr.addField("call_end", callEnd.toDate());
				cdr.addField("call_duartion", callDuartion);
				cdr.addField("call_cost", callCost);
				cdr.addField("recipient_network", recipientNnetwork);
				
			} else if (cdrDataType == 3){ //This is for SMS
				
			}
			
			// Commit the Solr Document
			server.add(cdr);
			server.commit();
			if(i == 710)
				break;
			if(i% 100 == 0)
				LOGGER.info("The counter: "+i);
		}
	}
	
	/**
	 * Method to search the indexed data from Solr
	 * @throws SolrServerException
	 */
	public void queryData() throws SolrServerException {

		server = new HttpSolrServer(url);
		SolrQuery query = new SolrQuery();
		query.setQuery("*:*");
		query.setStart(0);
		query.setRows(1000);
		QueryResponse response = server.query(query);
		SolrDocumentList results = response.getResults();
		LOGGER.info("Number of indexed records: " + results.getNumFound());
	}

	/** 
	 * Method to delete the indexed data from Solr
	 * @throws SolrServerException
	 * @throws IOException
	 */
    public void deleteAllIndexData() throws SolrServerException {
		server = new HttpSolrServer(url);
		try {
			server.deleteByQuery("*:*");
		} catch (IOException e) {
			LOGGER.error("Exception in purging Solr Data ",e);
		}//
		try {
			server.commit();
		} catch (IOException e) {
			LOGGER.error("Exception in purging Solr Data ",e);
		}
		LOGGER.info("all the data are deleted");
	}

	/**
	 * Generate a random age that can be assigned to a Twitter user
	 * @return
	 */
	public static int generateAge() {
		int min = 13;
		int max = 70;
	    Random rand = new Random();
	    return rand.nextInt((max - min) + 1) + min;
	    
	}
	
	/**
	 * Generate a random City & State that can be assigned to a Twitter user
	 * @return
	 */
public static String generateUserLocation() {
		
		int min = 0;
		int max = 10;
	    Random rand = new Random();
	    int randomAge = rand.nextInt((max - min) + 1) + min;
	    return location.get(randomAge);
	}
	
	public static String getReceipientNetwork() {
		
		int min = 0;
		int max = 5;
	    Random rand = new Random();
	    int randomIndex = rand.nextInt((max - min) + 1) + min;
	    return calledPartyNetwork.get(randomIndex);
	}
	

	public static String getAppName() {
		
		int min = 0;
		int max = 8;
	    Random rand = new Random();
	    int randomIndex = rand.nextInt((max - min) + 1) + min;
	    return appList.get(randomIndex);
	}

	
	/**
	 * Get the age groups based on the age
	 * @param age
	 * @return
	 */
public static String getAgeGroup(int age){
		
		String generation = "";
				
		/**
		 * Generation Information
		 * 1 - Generation Z = 2000 to present (Up to 16 years)
		 * 2 - Millennials = 1981 to 1999(16 to 35 years)
		 * 3 - Generation X 1965 - 80 (35 years to 50 years)
		 * 4 - Baby Boomers 1946 - 64 (51 years to 69)
		 */
		 
		if(age < 16)
			generation = "Generation Z";
		else if(age >=16 && age <35)
			generation = "Millennials";
		else if(age >=35 && age <51)
			generation = "Generation X";
		else if(age >=51 && age <70)
			generation = "Baby Boomers";
		
		return generation;
	}
	
	public DateTime getDateTime(String epochTime){
		
		long millisecondsSinceEpoch = Long.parseLong(epochTime);
		return new DateTime(millisecondsSinceEpoch);//YYY-MM-DD
		
	}
	
	
public String getMobileNumber(int counter){
		
		String mobile = "";
		if(counter < 6233)
			mobile = "9052100567";
		else if (counter >= 6233 && counter < 8233)
			mobile = "7702303254";
		else if (counter >= 8233 && counter < 16000)
			mobile = "9052789567";
		else if (counter >= 16000 && counter < 100000)
			mobile = "9052567567";
		
		return mobile;
	}
	public static void main(String[] args) throws FileNotFoundException, IOException, SolrServerException {
		
		/**
		 * Command Line Arguments 1 for Usage, 2 for Calls, 3 for SMS - for each
		 * value the input file should be different
		 */
		
		SolrIndexer solrIndexer = new SolrIndexer();
		int cdrType = Integer.parseInt(args[0]);
		solrIndexer.indexCDRData(cdrType);
		
	}
	
	

}