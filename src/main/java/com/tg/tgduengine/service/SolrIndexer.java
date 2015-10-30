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

import com.tg.tgduengine.util.TSVReader;
import com.tg.tgduengine.util.RandomGen; 

public class SolrIndexer {

	public static String line;
	public static final String url = "http://ec2-52-32-54-95.us-west-2.compute.amazonaws.com:8983/solr/collection1"; //Solr Instance
    public static SolrServer server ;
    public static List<String> location;
    public static List<String> calledPartyNetwork; 
    static {
    	
    	  location = new ArrayList<String>(10);
    	  location.add("Newark,New Jersey");
    	  location.add("San Franciscp,California");
    	  location.add("San Antonio,Texas");
    	  location.add("Chicago,Illinois");
    	  location.add("Jacksonville,Florida");
    	  
    	  location.add("New York City,New York");
    	  location.add("Columbus,Ohio");
    	  location.add("Oklahoma City,Oklahoma");
    	  location.add("Austin,Texas");
    	  location.add("Charlotte,North Carolina");
    	  
    	  calledPartyNetwork = new ArrayList<String>(6);
    	  calledPartyNetwork.add("AT&T");
    	  calledPartyNetwork.add("Verizon");
    	  calledPartyNetwork.add("Lyca");
    	  calledPartyNetwork.add("T-Mobile");
    	  calledPartyNetwork.add("Sprint");
    	  calledPartyNetwork.add("CenturyLink");

    	  
    	  
    }
  
    
    
   /**
    * Method to index the CDR from a flat file
    * @throws FileNotFoundException
    * @throws IOException
    * @throws SolrServerException
    */
	public void indexCDRData() throws FileNotFoundException, IOException, SolrServerException {

		TSVReader tsvReader = new TSVReader();
		List<String[]> parsedDataList = tsvReader.someMethod();
		SolrServer server = new HttpSolrServer(url);
		RandomGen randomGen = new RandomGen();
		
		for(int i=0; i<parsedDataList.size(); i++){
			String[] cdrArrary = parsedDataList.get(i);
			String circleId = cdrArrary[0];
			DateTime dataUsageUsageStartTime = getDateTime(cdrArrary[1]);
			DateTime dataUsageUsageEndTime = dataUsageUsageStartTime.plusMinutes(10);
			String dateUsed = cdrArrary[2];
			
			SolrInputDocument cdr = new SolrInputDocument();
			cdr.addField("id", i);
			cdr.addField("circlet_id", circleId);
			cdr.addField("dataUsageUsageStartTime", dataUsageUsageStartTime);
			cdr.addField("dataUsageUsageEndTime", dataUsageUsageEndTime);
			
			if (dateUsed !=null)
				dateUsed = "1";
			cdr.addField("dateUsed", dateUsed);
			
			/** 
			 * Generate random data dd dhdh
			 */
			int age = generateAge();
			String ageGroup = getAgeGroup(age);
			cdr.addField("age", age);
			cdr.addField("agegroup", ageGroup);
			
			String mobile = getMobileNumber(i);
			cdr.addField("mobile", mobile);
			
			String location = generateUserLocation();
			String[] splitLocation = location.split(",");
			String city = splitLocation[0];
			String state =  splitLocation[1];
			cdr.addField("city", city);
			cdr.addField("state", state);
			
			/**
			 * Add Called Mobile Number and User'd Email Address
			 */
			long calledMobileNumber = randomGen.getMobileNum();
			String email = randomGen.getEmail();
			cdr.addField("calledMobileNumber", calledMobileNumber);
			cdr.addField("email", email);

			
			server.add(cdr);
			server.commit();
			
			if(i == 10)
				break;
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
		
		SolrIndexer solrIndexer = new SolrIndexer();
		solrIndexer.indexCDRData();
		//deleteAllIndexData();
		//query_data();
	}
	
	

}