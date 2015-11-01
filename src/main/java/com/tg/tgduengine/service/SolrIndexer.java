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

import com.tg.tgduengine.util.TSVReader;
import com.tg.tgduengine.util.RandomDate;
import com.tg.tgduengine.util.RandomGen; 

public class SolrIndexer {

	public static String line;
	public static final String url = "http://ec2-52-32-54-95.us-west-2.compute.amazonaws.com:8983/solr/collection1"; //Solr Instance
    public static SolrServer server ;
    public static List<String> location;
    public static List<String> calledPartyNetwork;
    public static List<String> cDRTypeList;
    public static List<String> appList;
    
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
	public void indexCDRData(int cdrType) throws FileNotFoundException, IOException, SolrServerException {

		TSVReader tsvReader = new TSVReader();
		List<String[]> parsedDataList = null;
		if(cdrType == 1)
			parsedDataList = tsvReader.parseData();
		else if(cdrType == 2)
			parsedDataList = tsvReader.parseCallData();
		SolrServer server = new HttpSolrServer(url);
		RandomGen randomGen = new RandomGen();
		
		
		String[] cdrArrary;
		String cat = "call";
		int cdr_type = cdrType; //Solr Filed 1
		String circle_id = ""; //Solr Field 2
		
		
		/**
		 * Sample Date Value is: 2013-12-31T04:40:00.000+05:30
		 * So the data type in solar should be datetime (TrieDateField )
		 */
		DateTime data_usage_start_time; //Solr Field 3 
		DateTime data_usage_end_time; //Solr Field 4 
		
		String data_used_string = "";
		int data_used = 1; //Solr Field 5. by default the data used is set to 1 MB
		int age; //Solr Field 6
		String age_group; //Solr Field 7
		String mobile; //Solr Field 8
		
		String location_string; 
		String[] split_location;
		String[] data_used_split; 
		
		String city; //Solr Field 9
		String state; //Solr Field 10
		String email; //Solr Field 11
		String mobile_app_name = ""; //Solr Filed 12
		
		
		long recepient_mobile = 1234567890; //Solr Field 13
		DateTime call_start = new DateTime(); //Solr Filed 14
		DateTime call_end = new DateTime(); //Solr Filed 15
		float call_duartion = 0.0F;// //Solr Filed 16
		float call_cost = 0.0F; //Solr Filed 17
		String recipient_network = ""; //Solr Filed 18
		
		
		
		for(int i=0; i<parsedDataList.size(); i++){
			SolrInputDocument cdr = new SolrInputDocument();
			
			//Fetch Parsed Data
			cdrArrary = parsedDataList.get(i);
			
			//Common Fields
			circle_id = cdrArrary[0];
			age = generateAge();
			age_group = getAgeGroup(age);
			mobile = getMobileNumber(i);
			location_string = generateUserLocation();
			split_location = location_string.split(",");
			city = split_location[0];
			state =  split_location[1];
			email = randomGen.getEmail();
			
			if(cdrType == 1)
				cdr.addField("id", i);
			else if(cdrType == 2)
				cdr.addField("id", i+2000000000);
			
			cdr.addField("circle_id", circle_id);
			cdr.addField("age", age);
			cdr.addField("agegroup", age_group);
			cdr.addField("mobile", mobile);
			cdr.addField("city", city);
			cdr.addField("state", state);
			cdr.addField("email", email);
			
			
			//Fields specific to Usage
			if (cdr_type == 1) { 
				
				if(cdrArrary[2] != null) {
					data_used_string = cdrArrary[2];
					data_used_split = data_used_string.split("\\.");
					data_used = Integer.parseInt(data_used_split[0]);

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
				else if (i >= 44000)
					sampleDate = "2015-10-28";
				
				data_usage_start_time = RandomDate.getRandomCallTime(sampleDate);
				data_usage_end_time = data_usage_start_time.plusMinutes(data_used);
				mobile_app_name = getAppName();
				
				cdr.addField("data_usage_start_time", data_usage_start_time.toDate());
				cdr.addField("data_usage_end_time", data_usage_end_time.toDate());
				cdr.addField("dataUsed", data_used);
				cdr.addField("mobile_app_name", mobile_app_name);
		
			} else if (cdr_type == 2){ //This is for call
				
				recepient_mobile = randomGen.getMobileNum();
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
				else if (i >= 600)
					sampleDate = "2015-10-28";
				
				
				int call_start_time = 1;
				if(cdrArrary[2] != null) {
					data_used_string = cdrArrary[2];
					data_used_split = data_used_string.split("\\.");
					call_start_time = Integer.parseInt(data_used_split[0]);

				}
				
				call_start = RandomDate.getRandomCallTime(sampleDate);
				call_start = call_start.plusHours(call_start_time);
				//Fetch Call End
				int call_end_time = 2;
				if(cdrArrary[3] != null) {
					data_used_string = cdrArrary[3];
					data_used_split = data_used_string.split("\\.");
					call_end_time = Integer.parseInt(data_used_split[0]);

				}
				call_end = call_start.plusHours(call_end_time);
				//Fetch Call Duration
				call_duartion = Minutes.minutesBetween(call_start, call_end).getMinutes();
				if(call_duartion ==0){
					call_duartion = 60;
					call_end = call_start.plusHours(1);
				}
				//Fetch Call Cost
				//Fetch recipient_network using 
				recipient_network = getReceipientNetwork();
				cdr.addField("cat", "call");
				cdr.addField("recepient_mobile", recepient_mobile);
				cdr.addField("call_start", call_start.toDate());
				cdr.addField("call_end", call_end.toDate());
				cdr.addField("call_duartion", call_duartion);
				cdr.addField("call_cost", call_cost);
				cdr.addField("recipient_network", recipient_network);
				
			} else if (cdr_type == 3){ //This is for SMS
				
			}
			
			// Commit the Solr Document
			server.add(cdr);
			server.commit();
			if(i == 710)
				break;
			if(i% 100 == 0)
				System.out.println("The counter: "+i);
		}
	}
	
	/**
	 * Method to search the indexed data from Solr
	 * @throws SolrServerException
	 */
    public static void query_data() throws SolrServerException {

		server = new HttpSolrServer(url);
		SolrQuery query = new SolrQuery();
		query.setQuery("*:*");
		//query.addFilterQuery("tweet_content:*love*");
		/*query.addFilterQuery("body:sprint");
		query.setFields("id", "wireless_service_provider", "body");
		query.set("dataType", "text_general");*/
		
		query.setStart(0);
		query.setRows(1000);
		QueryResponse response = server.query(query);
		SolrDocumentList results = response.getResults();
		System.out.println("Number of indexed records: " + results.getNumFound());
		/*for (int i = 0; i < results.size(); i++) {
			System.out.println(results.get(i));
		}*/
		
	}

	/** 
	 * Method to delete the indexed data from Solr
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public static void deleteAllIndexData() throws SolrServerException, IOException {
		SolrServer server = new HttpSolrServer(url);
		server.deleteByQuery("*:*");//
		server.commit();
		System.out.println("all the data are deleted");
	}

	/**
	 * Generate a random age that can be assigned to a Twitter user
	 * @return
	 */
	public static int generateAge() {
		int min = 13;
		int max = 70;
	    Random rand = new Random();
	    int randomAge = rand.nextInt((max - min) + 1) + min;
	    return randomAge;
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
		final String GEN_Z = "Generation Z";
		final String MILLENNIALS = "Millennials";
		final String GEN_X = "Generation X";
		final String BOOMERS = "Baby Boomers";
		
		/**
		 * Generation Information
		 * 1 - Generation Z = 2000 to present (Up to 16 years)
		 * 2 - Millennials = 1981 to 1999(16 to 35 years)
		 * 3 - Generation X 1965 - 80 (35 years to 50 years)
		 * 4 - Baby Boomers 1946 - 64 (51 years to 69)
		 */
		 
		if(age < 16)
			generation = GEN_Z;
		else if(age >=16 && age <35)
			generation = MILLENNIALS;
		else if(age >=35 && age <51)
			generation = GEN_X;
		else if(age >=51 && age <70)
			generation = BOOMERS;
		
		return generation;
	}
	
	public DateTime getDateTime(String epochTime){
		
		long millisecondsSinceEpoch = Long.parseLong(epochTime);
		DateTime dataUsageUsageTime = new DateTime(millisecondsSinceEpoch);//YYY-MM-DD
		//DateTime  dataUsageEndTime = dt.plusMinutes(10);
		// and here's how to get the String representation
		//final String timeString2 = dt.toString("MM/dd/YYYY HH:mm:ss");
		
		/*	System.out.println("Start Time: "+dt);
			System.out.println("End Time: "+dataisageEndTime);*/
	
		return dataUsageUsageTime;
	}
	
	
	public String getMobileNumber(int counter){
		
		String mobile = "";
		if(counter < 3233)
			mobile = "9052100567";
		else if (counter >= 3233 && counter < 5999)
			mobile = "7702303254";
		else if (counter >= 5999 && counter < 9134)
			mobile = "9052789567";
		else if (counter >= 9134 && counter < 14474)
			mobile = "9052567567";
		else if (counter >= 14474 && counter < 19934)
			mobile = "9345100567";
		else if (counter >= 19934 && counter < 24000)
			mobile = "3485210567";
		else if (counter >= 24000)
			mobile = "5605210023";
		
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
		//deleteAllIndexData();
		//query_data();
	}
	
	

}