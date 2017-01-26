/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

/**
 *
 * @author Ganesha
 */
public class HW3 {
    
    static Boolean  isdebug = false;
    
  /*  public SearchResults getUasserDetails(Date y_membersince,String review_condition, String review_value, String noofFriends_cond,String nooffriendsvalue,String avgstarscond,String avgstarsVal, String maincondition ) {
        SearchResults SearchResults = new SearchResults();
       
        
        
        
            
select y_name,Y_YELPING_SINCE, Y_AVERAGE_STARS
from y_user_tb yusr 
where yusr.Y_REVIEW_COUNT = ?
and yusr.Y_FRIENDS_COUNT = ?
and yusr.Y_AVERAGE_STARS =?
        
        Connection dbconnection;
          //dbconnection = YelpDBConnectionFactory.connectDatabase();
        //String query = " select y_name,Y_YELPING_SINCE, Y_AVERAGE_STARS,Y_FRIENDS_COUNT,y_id from y_user_tb yusr where ";
          
          
          DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
      	DBCollection dbcollection=db1.getCollection("YelpUser");
      	String datestr="2014-12-09T00:00:00Z";
      	System.out.println("start"+"new ISODate("+datestr+")");
      	                      String option ="$and";
      	                     
      	 BasicDBObject proj1 = new BasicDBObject();
      	
      	
      	
      	
      		
      	 
   
      	
      	List<String> userset ;
      List userResults = new ArrayList();
      String query = null ;
      
      int count=0; 
        try { 
        
        boolean firstwhereclause = true;
        String mainconditiona =" and ";
        
        if(isnotnullornotempty(maincondition)  )
        {
            maincondition = maincondition.trim();
        }
        else maincondition = mainconditiona;
        DateFormat df = new SimpleDateFormat("yyyy-mm-dd");
          String date_string = df.format(y_membersince);  
          System.out.println("date_string + "+date_string +"y_membersince  " + y_membersince );
          BasicDBObject reviewcountcondDBOB = null;
          BasicDBObject reviewcountcond1 = null;
          BasicDBObject yelpingsincecondDBOB = null;
          BasicDBObject yelpingsince = null;
          BasicDBObject friendscondDOB  = null;
          BasicDBObject friendsDOB = null;
          BasicDBObject averagestarscondDBOB = null;
          BasicDBObject averagestarscond1 = null;
        if(isnotnullornotempty(review_value) && isnotnullornotempty(review_condition) )
        {
            String re= " yusr.Y_YELPING_SINCE "+ " > " + "  '"+ date_string +"'";
            
            System.out.println("Re string = "+ re);
            

          	String yelpingcond = "$gt";
      		 yelpingsincecondDBOB = new BasicDBObject(yelpingcond,y_membersince);
          	 yelpingsince = new BasicDBObject("yelping_since",yelpingsincecondDBOB); 
          	
                  
            
        }
        
        if(isnotnullornotempty(review_value) && isnotnullornotempty(review_condition) )
        {
            
        	
        	String reviewcond = condstr(review_condition);
        	reviewcountcondDBOB = new BasicDBObject(reviewcond,review_value);
        	reviewcountcond1 = new BasicDBObject("review_count",reviewcountcondDBOB);
          	  
        }
              
        if(isnotnullornotempty(nooffriendsvalue) && isnotnullornotempty(noofFriends_cond) )
        {
        	String yelpingcond = condstr(noofFriends_cond);
        	 friendscondDOB = new BasicDBObject(yelpingcond,asList("$friends",nooffriendsvalue));
          	 friendsDOB = new BasicDBObject("friends",friendscondDOB);
          	
        }
         if(isnotnullornotempty(avgstarsVal) && isnotnullornotempty(noofFriends_cond) )
        {
        	 String starscond = "$gte";
       		averagestarscondDBOB = new BasicDBObject(starscond,avgstarsVal);
           	 averagestarscond1 = new BasicDBObject("average_stars",averagestarscondDBOB);    	
           	
        }
         
         
         DBObject optioncond = new BasicDBObject(option ,asList(reviewcountcond1,averagestarscond1,yelpingsince,friendsDOB));              
	       
         DBObject match = new BasicDBObject("$match",optioncond);
       	
      	 DBObject project = new BasicDBObject("$project", new BasicDBObject("name", 1)
      	 .append("average_stars", 1).append("review_count", 1).append("yelping_since", 1).append("_id", 0));
      	
      	System.out.println(match );
      	 AggregationOutput output = dbcollection.aggregate(match,project);
        query = match.toString();
      	userResults= new ArrayList<String>();
      	 for (DBObject result : output.results()) {
      	  System.out.println(result.toString());
      	userResults.add(result.toString());
      	  }	
      	
      
        
         
       }catch (Exception e)
       {
           e.printStackTrace();
          
       }finally{
        
           
        }
        System.out.println(query);
        SearchResults.setIsusersearchResults(true);
        SearchResults.numberofrecords=count;
        SearchResults.setUserSearchresults(userResults);
        SearchResults.Query = query;
        return SearchResults;
    }
	*/
    
    private String condstr(String review_condition) {
		
		String cond;
		if (review_condition.equalsIgnoreCase(">"))
			cond = "$gte";
		else if (review_condition.equalsIgnoreCase("="))
			cond = "$eqe";
		else 
			cond = "$lte";
		
		if(isdebug)System.out.println("Input = "+ review_condition+" output ="+cond);
		return cond;
	}
    public SearchResults getUserDetailsQuery(Date y_membersince,String review_condition, String review_value, String noofFriends_cond,String nooffriendsvalue,String avgstarscond,String avgstarsVal, String maincondition,String userVotesCond ,String uservotesValue) {
        SearchResults SearchResults = new SearchResults();
       
        
        
        /*
            
select y_name,Y_YELPING_SINCE, Y_AVERAGE_STARS
from y_user_tb yusr 
where yusr.Y_REVIEW_COUNT = ?
and yusr.Y_FRIENDS_COUNT = ?
and yusr.Y_AVERAGE_STARS =?
        */StringBuilder userResultStr =new StringBuilder();
        Connection dbconnection;
         // dbconnection = YelpDBConnectionFactory.connectDatabase();
        DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
      	DBCollection dbcollection=db1.getCollection("YelpUser");
      	String datestr="2014-12-09T00:00:00Z";
      	if(isdebug)System.out.println("start"+" ISODate("+datestr+")");
      	                     
      	 
        String query = "";
      List <String>userResults = new ArrayList<String>();
      int count=0; 
        try { 
        
        boolean firstwhereclause = true;
        String mainconditiona =" and ";
        
        if(isnotnullornotempty(maincondition)  )
        {
            maincondition = maincondition.trim();
            if(isdebug)System.out.println("maincondition="+maincondition);
            if ("and, or".equalsIgnoreCase(maincondition)){
            	maincondition =" and ";
            	if(isdebug)System.out.println("maincondition="+maincondition);
            };
        }
        
        else maincondition = mainconditiona;
        
        String option = maincondition.equalsIgnoreCase("and")?"$and":"$or";
        DateFormat df = new SimpleDateFormat("yyyy-mm-dd");
        String date_string = df.format(y_membersince);  
        if(isdebug)System.out.println("date_string + "+date_string +"y_membersince  " + y_membersince );
        BasicDBObject reviewcountcondDBOB = null;
        BasicDBObject reviewcountcond1 = null;
        BasicDBObject yelpingsincecondDBOB = null;
        BasicDBObject yelpingsince = null;
        BasicDBObject friendscondDOB  = null;
        BasicDBObject friendsDOB = null;
        BasicDBObject averagestarscondDBOB = null;
        BasicDBObject averagestarscond1 = null;
        BasicDBObject  uservotesDOB= null;
         userResultStr = new StringBuilder();
        if(isnotnullornotempty(date_string) && isnotnullornotempty(date_string) )
        {
        	String yelpingcond = "$gt";
     		 yelpingsincecondDBOB = new BasicDBObject(yelpingcond,y_membersince);
         	 yelpingsince = new BasicDBObject("yelping_since",yelpingsincecondDBOB); 
         	if(isdebug)System.out.println("yelpingsince  ="+yelpingsince);
        }
        
        if(isnotnullornotempty(review_value) && isnotnullgezero(review_value) && isnotnullornotempty(review_condition) )
        {

        	String reviewcond = condstr(review_condition);
        	reviewcountcondDBOB = new BasicDBObject(reviewcond,Integer.parseInt(review_value));
        	reviewcountcond1 = new BasicDBObject("review_count",reviewcountcondDBOB);
        	System.out.println("Reviw condition ="+reviewcountcond1);
          	
        }
              
        if(isnotnullornotempty(nooffriendsvalue) && isnotnullgezero(nooffriendsvalue) && isnotnullornotempty(noofFriends_cond) )
        {
        	String yelpingcond = condstr(noofFriends_cond);
       	 friendscondDOB = new BasicDBObject(yelpingcond,Double.parseDouble(nooffriendsvalue));
         	friendsDOB = new BasicDBObject("friendsCount",friendscondDOB);
         	if(isdebug)System.out.println("friendsDOB  ="+friendsDOB);
        }
        if(isnotnullornotempty(userVotesCond) && isnotnullgezero(uservotesValue) && isnotnullornotempty(userVotesCond) )
        {
        	String yelpingcond = condstr(userVotesCond);
       	 friendscondDOB = new BasicDBObject(yelpingcond,Integer.parseInt(uservotesValue));
         	 uservotesDOB = new BasicDBObject("totalVotes",friendscondDOB);
         	//System.out.println("uservotesDOB  ="+uservotesDOB);
        }
         if(isnotnullornotempty(avgstarsVal) && isnotnullgezero(avgstarsVal) && isnotnullornotempty(avgstarscond) )
        {
        	 String starscond = condstr(avgstarscond);
        		averagestarscondDBOB = new BasicDBObject(starscond,Integer.parseInt(avgstarsVal));
            	 averagestarscond1 = new BasicDBObject("average_stars",averagestarscondDBOB);    	
            	 System.out.println("averagestarscond1  ="+averagestarscond1);
        }
         
        //query to database 
        
        /* PreparedStatement statement;
            statement = dbconnection.prepareStatement(query);
            
           ResultSet rs =  statement.executeQuery();
           count=0;
           while(rs.next()) {
               UserDetails useDetails = new  UserDetails();
               useDetails.setYname(rs.getString("y_name"));
               java.util.Date sqlDate;
            sqlDate = new java.util.Date(rs.getDate("Y_YELPING_SINCE").getTime());
             df = new SimpleDateFormat("yyyy-MM");
             df.format(sqlDate);
               useDetails.setYelp_Since(df.format(sqlDate));
               
               useDetails.setAverage_stars(rs.getString("Y_AVERAGE_STARS"));
               
               useDetails.setFriends_count(rs.getString("Y_FRIENDS_COUNT"));
           useDetails.setYid(rs.getString("y_id"));
               userResults.add(useDetails);
               count++;
           }*/
         List<BasicDBObject> asList = new ArrayList<BasicDBObject>();
         asList.add(yelpingsince);//reviewcountcond1,averagestarscond1,friendsDOB
         if(null != reviewcountcond1) asList.add(reviewcountcond1);
         if(null != averagestarscond1) asList.add(averagestarscond1);
         if(null != friendsDOB) asList.add(friendsDOB);
         if(null != uservotesDOB) asList.add(uservotesDOB);
		DBObject optioncond = new BasicDBObject(option ,asList);              
         System.out.println("optioncond  ="+optioncond);
         System.out.println("option"+option);
         DBObject match = new BasicDBObject("$match",optioncond);
         System.out.println("match  ="+match);
      	 DBObject project = new BasicDBObject("$project", new BasicDBObject("name", 1)
      	 .append("average_stars", 1).append("review_count", 1).append("yelping_since", 1).append("_id", 0));
      	DBObject limoit = new BasicDBObject("$limit",200);
      	System.out.println(match );
      	 AggregationOutput output = dbcollection.aggregate(match,project,limoit);
      	System.out.println("output"+output);
        query = match.toString();
      	userResults= new ArrayList<String>();
      	 for (DBObject result : output.results()) {
      	System.out.println(result);
      	userResults.add(result.toString());
      	userResultStr.append(result.toString());
      	  }	
         
       }catch (Exception e)
       {
           e.printStackTrace();
          
       }finally{
        
        }
        System.out.println(query);
      /*  SearchResults.setIsusersearchResults(true);
        SearchResults.numberofrecords=count;
        SearchResults.setUserSearchresults(userResults);
        SearchResults.Query = query;*/
        System.out.println(query);
        SearchResults.setIsusersearchResults(true);
        SearchResults.numberofrecords=count;
        SearchResults.jason=userResultStr.toString();
        SearchResults.setUserSearchresults(userResults);
        SearchResults.setUserStr(userResults);
        SearchResults.Query = query;
        return SearchResults;
    }
    
    boolean  isnotnullornotempty (String value) {
        boolean val =false;   
        if ( value != null && (value != null && value.trim() != "")  ) {
              val = true;
        }
        return val;
                
    }
    boolean  isnotnullgezero (String value) {
        boolean val =false;   
        if ( value != null && (value != null && value.trim() != "")  ) {
             int va = Integer.parseInt(value);
             if (va > 0) 
            	 val = true;
              
              
        }
        return val;
                
    }
    
    
    public ArrayList getSubcatogeries(ArrayList categories) {
    
        ArrayList subcat = null;
          Connection dbconnection;
          dbconnection = YelpDBConnectionFactory.connectDatabase();
         String query = " select distinct S_SUB_CAT_NAME from Y_BUS_SUB_CATEGORY   ";
       subcat = new ArrayList();
      int count=0; 
        try {
            String incond =  formincondition(categories);
            System.out.println("incond = "+incond);
            if (incond.equalsIgnoreCase(""))
                 query = "where S_BUSINESS_CATAGEORY " + incond;
            
             PreparedStatement statement;
            statement = dbconnection.prepareStatement(query);
            
           ResultSet rs =  statement.executeQuery();
           count=0;
         
           while(rs.next()) {
               
               subcat.add((String)rs.getString("S_SUB_CAT_NAME"));
             
          
            
               count++;
           }
        } catch (Exception e)
       {
           e.printStackTrace();
          
       }finally{
        
            try 
            {dbconnection.close();
            
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }
 
        
        return subcat;
    
      
    }
    
    public String   formincondition(ArrayList condition) {
    
        StringBuilder sb = new StringBuilder();
        StringBuilder query =new StringBuilder ("");
        sb.append(" in ( ");
        int count=0;
        for (Iterator iterator = condition.iterator(); iterator.hasNext();) {
            String st = (String)iterator.next();
            st= st.replace("'", ",");
             sb.append("'"+st+"' ,");
                  count++;         
        }
        if(count> 0 )
        	sb.setLength(sb.length() - 1);
        sb.append(" )");
        if(count > 0){
          query = new StringBuilder(sb);
        }
     return query.toString();
    }
    
    
    
     public List<String> getBusinessDetails(ArrayList<String>  Cat, String checkinfrom, String checkinto, String noofcheckin_cond,String noofcheckinvalue,Date reviewfrom ,Date Reviewto, String Stars_cond ,String Stars_value,String votes_cond,String votes_value,String searchForComboBox 
    		 , Boolean isaddressselected,Double longitude,Double latitude,Boolean isproximityset, String proximity) {
        SearchResults SearchResults = new SearchResults();
        Connection dbconnection;
        dbconnection = YelpDBConnectionFactory.connectDatabase();
     
        MongoClient client;
        
        client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));

		// TODO Auto-generated method stub
    	MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpBusiness");
    	ArrayList<String> str = Cat;
    	
    	
    	
    	
    	String option;
		if(searchForComboBox == "AND"){
			option = "$all";
		}else{
			option = "$in";
		}
		String query="";
		
		
		FindIterable<Document> resultBussinessid = b_collection.find(new Document("categories",new Document(option, str))).projection(fields(include("business_id"), excludeId()));
		MongoCursor<Document> iterator = resultBussinessid.iterator();
		final List<String> categoryBuissnessList = new ArrayList<String>();
		resultBussinessid.forEach(new Block<Document> (){
	         	@Override
	 			public void apply(final Document document) {
	 				// TODO Auto-generated method stub
	         		System.out.println("document"+document);
	         		
	         		//if (document != null)System.out.println(document.toJson().toString().trim());
	         		
	         		JSONParser parser = new JSONParser();
	                JSONObject jsonObject;
	   			
	   				try {
						jsonObject = (JSONObject) parser.parse(document.toJson().toString().trim());
						String business_id = (String) jsonObject.get("business_id");
						categoryBuissnessList.add(business_id);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	   				
	               
	                //Votes Parsing
	                
	 			}
	         	});
    	
		query= query+queryCategoryQuery(str);
		List<String> reviewBuissnessList = null;
		reviewBuissnessList = getReviewBusiness(  reviewfrom , Reviewto,  Stars_cond , Stars_value, votes_cond, votes_value);
		
		System.out.println("reviewBuissnessList"+reviewBuissnessList);
		
		List<String> proximityBuissnessList = null;
		if (isaddressselected && isproximityset)
		{
			System.out.println("proximity" + proximity);
			proximityBuissnessList = proximityQuery( longitude ,  latitude, Integer.parseInt(proximity));
			
			System.out.println("proximityBuissnessList" + proximityBuissnessList);
			query = query + proxqueryonly(longitude, latitude,Integer.parseInt(proximity));
		}
		List<String> CheckinBuissnessList = null;
		if(!noofcheckinvalue.equalsIgnoreCase("0")) {
	    	 
	    	 
	    	
	    	  
	    	  CheckinBuissnessList = getcheckinbusiness( categoryBuissnessList,Integer.parseInt(checkinfrom) ,Integer.parseInt(checkinto), Integer.parseInt(noofcheckinvalue));
	    	  
	     }
		System.out.println("categoryBuissnessList before merging"+categoryBuissnessList.size()+"    jfcjvkbkk  "+searchForComboBox);
		if (reviewBuissnessList!=null)System.out.println("categoryBuissnessList before merging"+reviewBuissnessList.size());
		if (proximityBuissnessList!=null)System.out.println("categoryBuissnessList before merging"+proximityBuissnessList.size());
		if (categoryBuissnessList!=null)System.out.println("categoryBuissnessList before merging"+categoryBuissnessList);
		Set ptempimityBuissnessList =new  HashSet<String>();
		if(searchForComboBox.equalsIgnoreCase("or")) {
			//do union
			System.out.println("in if ");
			//col.addAll(otherCol)// for union
			if (reviewBuissnessList!=null && !votes_value.equalsIgnoreCase("0")) categoryBuissnessList.addAll(reviewBuissnessList);
			if (proximityBuissnessList!=null) categoryBuissnessList.addAll(proximityBuissnessList);
			
			
		}else{
			//intersection
			System.out.println("in else ");
			
			
			//col.retainAll(otherCol) // for intersection
			if (reviewBuissnessList!=null) categoryBuissnessList.retainAll(reviewBuissnessList);
			if (proximityBuissnessList!=null) categoryBuissnessList.retainAll(proximityBuissnessList);

            
		}
		if (reviewBuissnessList!=null)System.out.println("categoryBuissnessList before merging"+reviewBuissnessList.size());
		if (proximityBuissnessList!=null)System.out.println("categoryBuissnessList before merging"+proximityBuissnessList.size());
		if (categoryBuissnessList!=null)System.out.println("categoryBuissnessList before merging"+categoryBuissnessList.size());
		if (categoryBuissnessList!=null)System.out.println("categoryBuissnessList"+categoryBuissnessList.size());
		System.out.println("categoryBuissnessList"+categoryBuissnessList);
		
    	FindIterable<Document> result = b_collection.find(new Document("business_id",new Document(option, categoryBuissnessList))).projection(fields(include("name", "city", "stars", "state"), excludeId())).limit(100);
    	 /*iterator = result.iterator();
    	while(iterator.hasNext()){
    		if(isdebug)System.out.println(iterator.next());
    	}*/
    	final List<String> businesses = new ArrayList<String>();
         result.forEach(new Block<Document> (){
         	@Override
 			public void apply(final Document document) {
 				// TODO Auto-generated method stub
         		businesses.add(document.toJson().toString().trim());
         		System.out.println(document.toJson().toString().trim());
 			}
         	});
         
         
	/*
      		
      		if( subCat== null || (subCat != null &&subCat.size() == 0) )  {
      			
      			subCat = getSubcatogeries(Cat);
      		}
      			System.out.println("got subcat"+subCat);
      
      
    List userResults = new ArrayList();
    int count=0; 
    String query = "";
      try { 
      
      boolean firstwhereclause = true;//+ "      where ";
      
      String  checkinquery ="";
     if(!noofcheckinvalue.equalsIgnoreCase("0")) {
    	 checkinquery = " SELECT bid   FROM ( SELECT bus.B_BID    AS bid,   SUM(chek.C_COUNT ) AS checkcount   FROM Y_BUSINESS_TB bus JOIN Y_CHECKIN_TB chek  ON chek.C_B_ID =bus.B_BID     JOIN Y_BUS_SUB_CATEGORY subcat      ON bus.B_BID = subcat.B_ID"
    			    	+" WHERE ";
    	 
    	  String cin = formincondition(subCat);
    	  checkinquery =  checkinquery +" subcat.S_SUB_CAT_NAME  " +cin +"  and chek.C_INFO  between '" +checkinfrom+"' and '"+checkinto+"'  "
    			  + " group by bus.B_BID)  where checkcount " +noofcheckin_cond+" "+noofcheckinvalue;
    	  
    	  
     }
     
     String  reviewquery ="";
    if(reviewfrom != null && Reviewto != null && ( !Stars_value.equalsIgnoreCase("0") && !votes_value.equalsIgnoreCase("0")) ) {
    	 String rin = formincondition(subCat);
    	reviewquery = " select bus.B_BID as  bid from Y_BUSINESS_TB bus  join  Y_REVIEWS_TB review on bus.B_BID = review.R_BUSS_ID "
     		+ "join Y_BUS_SUB_CATEGORY subcat on bus.B_BID = subcat.B_ID where subcat.S_SUB_CAT_NAME " +rin+ " ";
    
    	  DateFormat df = new SimpleDateFormat("dd-MMM-yyyy");
	        String date_string_reviewfrom = df.format(reviewfrom);  
	        String date_string_reviewto = df.format(Reviewto);  
	        
    	reviewquery = reviewquery+" and review.R_PUBLISH_DATE BETWEEN '"+date_string_reviewfrom+"' and '"+date_string_reviewto+"' ";
    	if ( !votes_value.equalsIgnoreCase("0") ) {

    		reviewquery = reviewquery+ " and review.R_VOTES " +votes_cond+ "  '" + votes_value+"' " ;
    		
    	}
    	if (  !Stars_value.equalsIgnoreCase("0")) {
    		
    		reviewquery = reviewquery+ " and  review.r_stars "+ Stars_cond+" '"+Stars_value+"'      ";
    	}
     	
   	  
    }
    query = " "
    		+ ""
    		+ "select B_BID,B_NAME,B_CITY,B_STATE,B_STARS from Y_BUSINESS_TB ab where ab.B_BID in (";
    System.out.println("reviewquery "+reviewquery.equalsIgnoreCase("")+" reviewquery");
    System.out.println("checkinquery "+checkinquery.equalsIgnoreCase("")+" checkinquery.equalsIgnoreCase(");
    
    
     if (reviewquery.equalsIgnoreCase("") && checkinquery.equalsIgnoreCase("")) 
    {
    	
    	 System.out.println("reviewquerasdasdy"+reviewquery+"reviewquery");
    	String subcatonly = " select B_ID from Y_BUS_SUB_CATEGORY  where S_SUB_CAT_NAME ";
    	String subcatin = formincondition(subCat);
    	subcatonly =subcatonly+subcatin;
    	query=query+subcatonly + " ) ";
    }
    
     else {		if (reviewquery.equalsIgnoreCase("")){
    	
    	 				query = query+checkinquery+ " ) ";
     			}
     			else if (checkinquery.equalsIgnoreCase("")){
    	
     					query = query+reviewquery+ " ) ";
     			}
    
     			else {
     				query = query+checkinquery+ " INTERSECT "+reviewquery+ " ) ";
     			}
      
     }
      
       
         System.out.println("query"+query);
         
         }catch (Exception e){
       
           e.printStackTrace();
       }
     //   System.out.println(query);
     //   SearchResults.setIsusersearchResults(true);
     //   SearchResults.numberofrecords=count;
     //   SearchResults.setUserSearchresults(userResults);
     //   SearchResults.Query = query;
*/        return businesses;
    }
	private String queryCategoryQuery(ArrayList<String> str) {
		BasicDBObject inQuery = new BasicDBObject();
		BasicDBObject fields = new BasicDBObject();
		fields.put("business_id", 1);
		fields.put("name", 1);
		fields.put("state", 1);
		fields.put("city", 1);
		fields.put("stars", 1);
		fields.put("_id", 0);

		inQuery.put("categories", new BasicDBObject("$in", str));
		return inQuery.toString();
	}
	private String proxqueryonly(Double longitude, Double latitude,Integer miles) {
		BasicDBObject coordinates = new BasicDBObject("coordinates",asList(longitude,latitude));
		BasicDBObject type = new BasicDBObject("type","Point").append("coordinates",asList(longitude,latitude));
		BasicDBObject geometry = new BasicDBObject("$geometry",type);
		System.out.println(geometry);
		double maxdistancekeyenin = 1609.34 *miles ;
		BasicDBObject maxdistane = new BasicDBObject("$maxDistance",maxdistancekeyenin);
		BasicDBObject near = new BasicDBObject("$near",geometry.append("$maxDistance",maxdistancekeyenin));
		
		BasicDBObject location = new BasicDBObject("loc",near);
		String Query = location.toString();
		return Query;
	}
     static String getcheckininfo(JSONObject obj , String index )
     {
         String t0_1 = null ;
         if (obj.get(index) != null)
             t0_1 = obj.get(index).toString();
         return t0_1;
     }
     private  List<String> getcheckinbusiness(List businessids,Integer fromdate,Integer todate, Integer keyedin_count) {
 		System.out.println("checkinSEARCH " );
 	
 		MongoClient client;
 		String searchForComboBox ="and";
 		System.out.println(businessids);
 	    DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
       	DBCollection dbcollection=db1.getCollection("YelpCheckin");
       	List<String> cbeckinBusinessList = null;
 	    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
 	/*	System.out.println(asList(match(eq("name", "Tyler")), 
 				lookup("YelpReview", "business_id", "business_id", "YelpBusinessReviews"), 
 				unwind("$YelpBusinessReviews"),
 				
 				project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "name"), computed("usefulVotes", "votes.useful"),  excludeId())),
 				unwind("$userName")));*/
 		MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpCheckin");
 	    /*	db.YelpUser.aggregate([
 	    	                       { 
 	    	                         $match: {
 	    	                              $and: [ 
 	    	                                  { business_id: { $in: [ 5, 15 ] } }
 	    	                                  {date: {$gt: 3,$lt :5}}, 
 	    	    							  {stars: {$gt: 3}}, 
 	    	                                  {$votes.useful: {$gt:2}},
 	    	    								
 	    	    							   
 	    	                              ]
 	    	                         }
 	    	                       }
 	    	                      ])*/
 		
       	FindIterable<Document> result = b_collection.find(new Document("business_id",new Document("$in", businessids))).projection(fields(include("business_id", "checkin_info"), excludeId()));
     	System.out.println("   asdas ="+result );
     	MongoCursor<Document> iterator = result.iterator();
     	
     	 final List<String> checkinJson = new ArrayList<String>();
          result.forEach(new Block<Document> (){
          	@Override
  			public void apply(final Document document) {
  				// TODO Auto-generated method stub
          		checkinJson.add(document.toJson().toString().trim());
          		if(isdebug)System.out.println(document.toJson().toString().trim());
  			}
          	});		
          System.out.println(""+checkinJson);
          
          
          //got jasonobjects
          try {
          Iterator<String> iterator2 = checkinJson.iterator();
          HashMap<String, Integer> cbeckincountmap = new HashMap<String, Integer>();
          while (iterator2.hasNext())
          {
              JSONParser parser = new JSONParser();
              JSONObject jsonObject;
 			
 				jsonObject = (JSONObject) parser.parse(iterator2.next());
 			
             
              //Votes Parsing
              String business_id = (String) jsonObject.get("business_id");
             
              JSONObject checkininfo = (JSONObject) jsonObject.get("checkin_info");
              String [][]checkin = new String[24][7];
               String [][]checkinT = new String[7][24];
             int  checkincount= 0;
             String c_info = null;
              for (int i=0 ;i<24 ; i++){
                  for (int j=0;j<7;j++) {
                      checkin[i][j] = getcheckininfo(checkininfo,i+"-"+j);
                     c_info =  getcheckininfo(checkininfo,i+"-"+j);
                     
                      if (c_info != null ) {

                          String dayhour =  (i>=10)?""+j+i :  j+"0"+i;
                         Integer dayhourInt= Integer.parseInt(dayhour);
                         Integer c_infoInt= Integer.parseInt(c_info);
                         /* p_statement.setNString(2, dayhour);
                          p_statement.setNString(3, c_info);//count
      */                    
                 		 
 						
                     	 if (dayhourInt > fromdate && dayhourInt<todate) {
                     		 Integer ccount = cbeckincountmap.get(business_id);
 		                    	 if (ccount != null) {
 		                    		 Integer existing_value =  cbeckincountmap.get(business_id);
 	                    	    	 cbeckincountmap.put(business_id,existing_value+c_infoInt);
 		                    		
 		                    	 } else {
 		                    	     
 		                    		 cbeckincountmap.put(business_id,c_infoInt);
 		                    	     
 		                    	 }
                     	 }
                           
                          
                          checkincount++;
                         
                          //make changes  to checkin to hold day and then hours
                          // also store the count.
                          
                          //System.out.println(" i = "+i +"J = "+j + "Value "+c_info);
                      }
                    
                  //System.out.println("checkin [][]= "+i+" "+j+" "+checkin[i][j]);
                  }
              }
              //System.out.println("checkin [][]= "+checkin);
              
           
           // checkinT = ParseJson.transpose(checkin);
            
              
              
             
//            
//              count++;
//              System.out.println(count+"\n");
          }
          
          
          System.out.println("cbeckincountmap"+cbeckincountmap);
          cbeckincountmap.keySet();
          cbeckinBusinessList = new ArrayList<String>();
          Integer count;
          for (String string : cbeckincountmap.keySet()) {
         	  count = cbeckincountmap.get(string);
         	  if(count>keyedin_count) {
         		  cbeckinBusinessList.add(string);
         	  }
 		}
          
          
          } catch (ParseException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			}
          System.out.println("Done executing checkin");
          return cbeckinBusinessList;
 	}
     private static List proximityQuery(double longitude , double latitude,int miles) {
 		List<String> result = new ArrayList<>();
 		MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
 		//MongoCollection<Document> db = client.getDatabase("YelpApplication").getCollection("YelpBusiness");
 		DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
     	//DBCollection dbcollection=db1.getCollection("YelpBusiness");
     	BasicDBObject q = new BasicDBObject();
     	DBCollection dbcollection=db1.getCollection("YelpBusiness");
     	BasicDBObject proj1 = new BasicDBObject();	
     	proj1.put("business_id", 1);
     	
     	proj1.append("_id", 0);
     	/* db.YelpBusiness.find(
     			   {
     			     loc:
     			       { $near :
     			          {
     			            $geometry: { type: "Point",  coordinates: [ -89.3083801269531, 43.1205749511719 ] },
     			          
     			            $maxDistance: 80467.54
     			          }
     			       }
     			   }
     			)
     			{ "loc" : { "$near" : { "$geometry" : { "type" : "Point" , "coordinates" : [ -89.3083801269531 , 43.1205749511719]}} , "$maxDistance" : 1609.34}}
     			
     			{ "loc" : { "$near" : { "$geometry" : { "type" : "Point" , "coordinates" : [ -89.3083801269531 , 43.1205749511719]} , "$maxDistance" : 1609.34}}}
     			*/
     	if(miles<0)
     	 miles=1;
 		
     	
     	BasicDBObject coordinates = new BasicDBObject("coordinates",asList(longitude,latitude));
     	BasicDBObject type = new BasicDBObject("type","Point").append("coordinates",asList(longitude,latitude));
     	BasicDBObject geometry = new BasicDBObject("$geometry",type);
     	System.out.println(geometry);
     	double maxdistancekeyenin = 1609.34 *miles ;
 		BasicDBObject maxdistane = new BasicDBObject("$maxDistance",maxdistancekeyenin);
 		BasicDBObject near = new BasicDBObject("$near",geometry.append("$maxDistance",maxdistancekeyenin));
 		
 		BasicDBObject location = new BasicDBObject("loc",near);
 		System.out.println(location);
 		DBCursor resultset = dbcollection.find(location,proj1);

 		  while(resultset.hasNext()){
	    	   String str = (String)resultset.next().toString();
	    	   JSONParser parser = new JSONParser();
				try {
					JSONObject jsonObject = (JSONObject) parser.parse(str.toString().trim());
					
	    	    	 String bid = (String)jsonObject.get("business_id");
	    	       	if(isdebug)System.out.println(bid);
	    	       	result.add(bid);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	   
	       }
     	
     	/*MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpBusinessAttributes");
     	FindIterable<Document> result = b_collection.find(new Document("categories",new Document(option, str)).append("attribute_keys", new Document(option, str2))).projection(fields(include("name", "city", "stars", "state"), excludeId()));
         List<String> businesses = new ArrayList<String>();
         result.forEach(new Block<Document> (){
         	@Override
 			public void apply(final Document document) {
 				// TODO Auto-generated method stub
         		businesses.add(document.toJson().toString().trim());
         		System.out.println(document.toJson().toString().trim());
 			}
         	});*/
 		 System.out.println("Done executing proximity");
 		return result;
 	}
  
     public List getReviewBusiness(Date reviewfrom ,Date Reviewto, String Stars_cond ,String Stars_value,String votes_cond,String votes_value){
 		MongoClient client;
 		client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
 	    DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
       	DBCollection dbcollection=db1.getCollection("YelpReview");
       	ArrayList businessiidList = null;
 		String searchForComboBox ="and";
 		BasicDBObject votescountcondDBOB = null;
 		BasicDBObject votescountcond1 = null;
 		BasicDBObject averagestarscondDBOB =null;
 		BasicDBObject averagestarscond1 = null;
 		BasicDBObject begincondDBOB  = null;
 		BasicDBObject rdatecondDBOB = null;
 		if(reviewfrom != null && Reviewto != null && ( !Stars_value.equalsIgnoreCase("0") && !votes_value.equalsIgnoreCase("0")) ) {
 	    	  
 	    	 DateFormat df = new SimpleDateFormat("dd-MMM-yyyy");
 		        String date_string_reviewfrom = df.format(reviewfrom);  
 		        String date_string_reviewto = df.format(Reviewto);  
 		       String rdatecond = "$gt";
 	 			 begincondDBOB = new BasicDBObject("$gt", reviewfrom).append("$lt", Reviewto);//" ISODate("+datestr+")"
 	 			 rdatecondDBOB = new BasicDBObject("date",begincondDBOB); 
 	 			System.out.println("rdatecondDBOB"+rdatecondDBOB);
 	    	if ( !votes_value.equalsIgnoreCase("0") ) {

 	    		String reviewcond = condstr(votes_cond);
 	 			 votescountcondDBOB = new BasicDBObject(reviewcond,Double.parseDouble(votes_value));
 	 			 votescountcond1 = new BasicDBObject("totalVotes",votescountcondDBOB);
 	    	}
 	    	if (  !Stars_value.equalsIgnoreCase("0")) {
 	    		
 	    		
 	 			String starscond = condstr(Stars_cond);
 	 			 averagestarscondDBOB = new BasicDBObject(starscond,Integer.parseInt(Stars_value));
 	 			 averagestarscond1 = new BasicDBObject("stars",averagestarscondDBOB); 
 	    	}
 	    	String datestr="2014-12-09T00:00:00Z";
			System.out.println("start"+"new ISODate("+datestr+")");
		    String option ="$and";
		   
		BasicDBObject proj1 = new BasicDBObject();
		
		
		 List<BasicDBObject> asList = new ArrayList<BasicDBObject>();
     // asList.add(rdatecondDBOB);//reviewcountcond1,averagestarscond1,friendsDOB
      if(null != averagestarscond1) asList.add(averagestarscond1);
      if(null != rdatecondDBOB) asList.add(rdatecondDBOB);
      if(null != votescountcond1) asList.add(votescountcond1);
      
      DBObject optioncond = new BasicDBObject(option ,asList);  
 	    /*	db.YelpUser.aggregate([
 	    	                       { 
 	    	                         $match: {
 	    	                              $and: [ 
 	    	                                   
 	    	                                  {date: {$gt: 3,$lt :5}}, 
 	    	    							  {stars: {$gt: 3}}, 
 	    	                                  {$votes.useful: {$gt:2}},
 	    	    								
 	    	    							   
 	    	                              ]
 	    	                         }
 	    	                       }
 	    	                      ])*/
 				            
 	         System.out.println("optioncond  ="+optioncond);
 	         System.out.println("option"+option);
 	         DBObject match = new BasicDBObject("$match",optioncond);
 	         System.out.println("match  ="+match);
 	      	 DBObject project = new BasicDBObject("$project", new BasicDBObject("business_id", 1).append("_id", 0));
 	      	
 	         System.out.println(match );
 	      	 AggregationOutput output = dbcollection.aggregate(match,project);
 	      	System.out.println("output"+output);
 	       // query = match.toString();
 	      	 businessiidList= new ArrayList<String>();
 	      	 for (DBObject result : output.results()) {
 	      	  System.out.println("reviews"+result);
 	      	businessiidList.add(result.get("business_id"));
 	      	//System.out.println(businessiidList);
 	      	 }
 	   	  
 	    }
 		System.out.println("Done executing review");
 	      	return businessiidList;
 	      	
 	}
}


