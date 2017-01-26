package com.samples;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
public class test
{

	public static void main(String[] args)
	{
		//userQuery();
		//proximityQuery();
		//categoerysearch();
		//review();
		//reviewSEARCH();
		 getReviewBusiness(new Date("01/01/2013") ,new Date("10/01/2013"), ">" ,"3", ">" ,"3");
		/*List categoerysearch = categoerysearch();
		checkinSEARCH(categoerysearch);*/
		//reviewanduserSEARCH();
			System.out.println("done " );
	}
	private static List<String> checkinSEARCH(List businessids) {
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
         		System.out.println(document.toJson().toString().trim());
 			}
         	});		
         System.out.println(""+checkinJson);
         
         Integer fromdate = 010;
         Integer todate =120;
         int keyedin_count = 3;
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
                         
                         System.out.println(" i = "+i +"J = "+j + "Value "+c_info);
                     }
                   
                 //System.out.println("checkin [][]= "+i+" "+j+" "+checkin[i][j]);
                 }
             }
             //System.out.println("checkin [][]= "+checkin);
             
          
          // checkinT = ParseJson.transpose(checkin);
           
             
             
            
//           
//             count++;
//             System.out.println(count+"\n");
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
         return cbeckinBusinessList;
	}
	  static String getcheckininfo(JSONObject obj , String index )
     {
         String t0_1 = null ;
         if (obj.get(index) != null)
             t0_1 = obj.get(index).toString();
         return t0_1;
     }
	private static void reviewSEARCH() {
		MongoClient client;
		String searchForComboBox ="and";
		
	    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
	    DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
      	DBCollection dbcollection=db1.getCollection("YelpReview");
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
					String datestr="2014-12-09T00:00:00Z";
				System.out.println("start"+"new ISODate("+datestr+")");
			    String option ="$and";
			   
			BasicDBObject proj1 = new BasicDBObject();
			
			String reviewcond = "$gte";
			BasicDBObject votescountcondDBOB = new BasicDBObject(reviewcond,3);
			BasicDBObject votescountcond1 = new BasicDBObject("totalVotes",votescountcondDBOB);
			
			String starscond = "$gte";
			BasicDBObject averagestarscondDBOB = new BasicDBObject(starscond,3);
			BasicDBObject averagestarscond1 = new BasicDBObject("stars",averagestarscondDBOB);    	
			
			String rdatecond = "$gt";
			BasicDBObject begincondDBOB = new BasicDBObject("$gt", new Date ("10/10/2013")).append("$lt", new Date ("01/01/2014"));//" ISODate("+datestr+")"
			BasicDBObject rdatecondDBOB = new BasicDBObject("date",begincondDBOB); 
			System.out.println("rdatecondDBOB"+rdatecondDBOB);
			
			/*
			 * 
			 { "$match" : { "$and" : [ 
			 { "date" : { "$gt" : { "$date" : "2013-10-10T07:00:00.000Z"}} , "$lt" : { "$date" : "2014-01-01T08:00:00.000Z"}} , { "stars" : { "$gte" : 3}} , { "$votes.useful" : { "$gte" : 3}}]}}
			 * 
			 * { "$match" : { "$and" : [ { "date" : { "$gt" : { "$date" : "2013-10-10T07:00:00.000Z"} , "$lt" : { "$date" : "2014-01-01T08:00:00.000Z"}}} , { "stars" : { "$gte" : 3}} , { "$votes.useful" : { "$gte" : 3}}]}}
			 * { "$match" : { "$and" : [ { "date" : { "$gt" : { "$date" : "2013-10-10T07:00:00.000Z"}} , "$lt" : { "$date" : "2014-01-01T08:00:00.000Z"}}
			 *  , { "stars" : { "$gte" : 3}} ,  { "$votes.useful" : { "$gte" : 3}}]}}
			//{ "friends": { "$gte" : [ "$friends", "3" ] }}
			//
*/			
			 List<BasicDBObject> asList = new ArrayList<BasicDBObject>();
	         asList.add(rdatecondDBOB);//reviewcountcond1,averagestarscond1,friendsDOB
	         if(null != averagestarscond1) asList.add(averagestarscond1);
	         //if(null != rdatecondDBOB) asList.add(rdatecondDBOB);
	         if(null != votescountcond1) asList.add(votescountcond1);
	         
	         DBObject optioncond = new BasicDBObject(option ,asList);              
	         System.out.println("optioncond  ="+optioncond);
	         System.out.println("option"+option);
	         DBObject match = new BasicDBObject("$match",optioncond);
	         System.out.println("match  ="+match);
	      	 DBObject project = new BasicDBObject("$project", new BasicDBObject("business_id", 1).append("_id", 0));
	      	
	         System.out.println(match );
	      	 AggregationOutput output = dbcollection.aggregate(match,project);
	      	System.out.println("output"+output);
	       // query = match.toString();
	      	ArrayList businessiidList= new ArrayList<String>();
	      	 for (DBObject result : output.results()) {
	      	  System.out.println(result);
	      	businessiidList.add(result.get("business_id"));
	      	System.out.println(businessiidList);
	      	
	      	  }	
	}
	private static void reviewanduserSEARCH() {
		MongoClient client;
		String searchForComboBox ="and";
		
	    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
	    DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
      	DBCollection dbcollection=db1.getCollection("YelpUser");
      	
      	BasicDBObject proj1 = new BasicDBObject();
		
		String reviewcond = "$gte";
		  String option ="$and";
		BasicDBObject votescountcond1 = new BasicDBObject("name","Bill");
		DBObject optioncond = new BasicDBObject(option ,asList(votescountcond1));              
        System.out.println("optioncond  ="+optioncond);
        System.out.println("option"+option);
        DBObject match = new BasicDBObject("$match",optioncond);
        System.out.println("match  ="+match);
        DBObject project = new BasicDBObject("$project", new BasicDBObject("user_id", 1).append("_id", 0));
      	
        System.out.println(match );
     	 AggregationOutput output = dbcollection.aggregate(match,project);
     	//System.out.println("output"+output);
      // query = match.toString();
     	String  user_id= "";
     	 for (DBObject result : output.results()) {
     	 
     	
     	JSONParser parser = new JSONParser();
        JSONObject jsonObject;
		
			try {
			jsonObject = (JSONObject) parser.parse(result.toString().trim());
			String business_id = (String) jsonObject.get("user_id");
			user_id=business_id;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     	
     	  }	
     	 
     	 
     	 System.out.println("user_id"+user_id);
      
				dbcollection=db1.getCollection("YelpReview");
			 proj1 = new BasicDBObject();
			
			 reviewcond = "$gte";
			 BasicDBObject votescountcondDBOB = new BasicDBObject("$eq",user_id);
			 votescountcond1 = new BasicDBObject("user_id",votescountcondDBOB);
			
			
			
			
			 List<BasicDBObject> asList = new ArrayList<BasicDBObject>();
	         
	         //if(null != rdatecondDBOB) asList.add(rdatecondDBOB);
	         if(null != votescountcond1) asList.add(votescountcond1);
	         
	          optioncond = new BasicDBObject(option ,asList);              
	         System.out.println("optioncond  ="+optioncond);
	         System.out.println("option"+option);
	          match = new BasicDBObject("$match",optioncond);
	         System.out.println("match  ="+match);
	      	  project = new BasicDBObject("$project", new BasicDBObject("text", 1).append("stars", 1).append("totalVotes", 1).append("date", 1).append("_id", 0));
	      	
	         System.out.println(match );
	      	  output = dbcollection.aggregate(match,project);
	      	System.out.println("output"+output);
	       // query = match.toString();
	      	ArrayList businessiidList= new ArrayList<String>();
	      	 for (DBObject result : output.results()) {
	      	  System.out.println(result);
	      	  JSONParser parser = new JSONParser();
              JSONObject jsonObject;
	   			
 				
	      	
	      	  }	
	}
	private static void review() {
		MongoClient client;
		String searchForComboBox ="and";
	    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
	/*	System.out.println(asList(match(eq("name", "Tyler")), 
				lookup("YelpReview", "business_id", "business_id", "YelpBusinessReviews"), 
				unwind("$YelpBusinessReviews"),
				
				project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "name"), computed("usefulVotes", "votes.useful"),  excludeId())),
				unwind("$userName")));*/
		MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpUser");
		//"_id", "5848ba5b0cd687c660bf66c2"
		
		AggregateIterable<Document> list = b_collection.aggregate(asList(match(eq("name", "Harley")), 
				lookup("YelpReview", "user_id", "user_id", "YelpBusinessReviews"), 
				unwind("$YelpBusinessReviews"),
				project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "$name"), computed("usefulVotes", "$votes.useful"),  excludeId()))
				
				));
		//project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "$name"), computed("usefulVotes", "$votes.useful"),  excludeId()))
		//,project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "$name"), computed("usefulVotes", "$votes.useful"),  excludeId()))
			System.out.println("b_collection"+b_collection);
			System.out.println(asList(match(eq("name", "Tyler")), 
				lookup("YelpReview", "user_id", "business_id", "YelpBusinessReviews"), 
				unwind("$YelpBusinessReviews"),
				
				project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "$name"), computed("usefulVotes", "$votes.useful"),  excludeId()))));
			try {
			for(Document str:list){
			System.out.println("list is: "+str.toJson());
			/*JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject) parser.parse(str.toJson().toString().trim());
			String date = (String) jsonObject.get("date");
			String stars = (String) jsonObject.get("stars").toString();
			String text = (String) jsonObject.get("text");
			String user_name = (String) jsonObject.get("userName");
			String votesUseful=(String) jsonObject.get("usefulVotes").toString();
			*/
			}
			}
			catch(Exception e){
			System.out.println("Cant execute " + e);
			e.printStackTrace();
			}
	}
	private static  List categoerysearch()
	{ 
		MongoClient client;
	String searchForComboBox ="and";
    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));

	// TODO Auto-generated method stub
	MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpBusiness");
	ArrayList<String> str = new ArrayList<String>();//Cat;
	
	String[] allCategories = {"Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists", "Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical", "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening", "Nightlife", "Restaurants", "Shopping", "Transportation"};
	for (int i = 0; i < 1; i++) {
		str.add(allCategories[i]); 
	}
	for(String str3: str){
		System.out.println(str3);
	}
	
	
	String option;
	if(searchForComboBox == "AND"){
		option = "$all";
	}else{
		option = "$in";
	}
	//new Document("categories",new Document(option, str))).projection(fields(include("name", "city", "stars", "state"), excludeId())
	System.out.println("   asdas =");
	FindIterable<Document> result = b_collection.find(new Document("categories",new Document(option, str))).projection(fields(include("business_id"), excludeId()));
	System.out.println("   asdas ="+result );
	MongoCursor<Document> iterator = result.iterator();
	
	 final List<String> businesses = new ArrayList<String>();
    result.forEach(new Block<Document> (){
    	@Override
			public void apply(final Document document) {
				// TODO Auto-generated method stub
    		JSONParser parser = new JSONParser();
			try {
				JSONObject jsonObject = (JSONObject) parser.parse(document.toJson().toString().trim());
				
    	    	 String bid = (String)jsonObject.get("business_id");
    	       	System.out.println(bid);
    	       	
    	       	
    	       	businesses.add(bid);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		
    		System.out.println(document.toJson().toString().trim());
			}
    	});
	 /*for (DBObject result : output.results()) {
     	  System.out.println(result);
     	businessiidList.add(result.get("business_id"));
     	System.out.println(businessiidList);
     	
     	  }	*/
	
     System.out.println(businesses); 
     return businesses;
     
	}
	private static List proximityQuery() {
		List<String> result = new ArrayList<>();
		MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		//MongoCollection<Document> db = client.getDatabase("YelpApplication").getCollection("YelpBusiness");
		DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
    	//DBCollection dbcollection=db1.getCollection("YelpBusiness");
    	BasicDBObject q = new BasicDBObject();
    	DBCollection dbcollection=db1.getCollection("YelpBusiness");
    	BasicDBObject proj1 = new BasicDBObject();	
    	//proj1.put("name", 1);
    	proj1.append("business_id", 1);
    	//proj1.append("stars", 1);
    	//proj1.append("state", 1);
    	//proj1.append("_id", 0);
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
    	
    	int miles=10;
		
    	String reviewcond = "$gte";
		BasicDBObject reviewcountcondDBOB = new BasicDBObject(reviewcond,3);
    	BasicDBObject reviewcountcond1 = new BasicDBObject("review_count",reviewcountcondDBOB);
    	
    	BasicDBObject coordinates = new BasicDBObject("coordinates",asList(-89.3083801269531,43.1205749511719));
    	BasicDBObject type = new BasicDBObject("type","Point").append("coordinates",asList(-89.3083801269531,43.1205749511719));
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
			    	       	System.out.println(bid);
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
		return result;
	}

	private static void userQuery() {
		// TODO Auto-generated method stub
		MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> db = client.getDatabase("YelpApplication").getCollection("YelpBusiness");
		
		BasicDBObject q = new BasicDBObject();
		
//		FindIterable<Document> result = db.find(new Document("stars", new Document("$eq", 3.5)));
//		result.forEach(new Block<Document>() {
//
//			@Override
//			public void apply(final Document document) {
//				// TODO Auto-generated method stub	
//				System.out.println(document.toJson().trim());
//			}
//		});
		
//		AggregateIterable<Document> list = db.aggregate(asList(new Document("$lookup", new Document("from", "YelpBusiness").append("localField", "business_id").append("foreignField", "business_id").append("as","YelpBusinessReviews"))));
//		
//		list.forEach(new Block<Document> (){
//
//			@Override
//			public void apply(final Document document) {
//				// TODO Auto-generated method stub
//				System.out.println(document.toJson().trim());
//			}
//			
//		});
		ArrayList<String> str = new ArrayList<String>();
		str.add("Restaurants");
		
		ArrayList<String> str2 = new ArrayList<String>();
		str2.add("Alcohol_full_bar");
//		Document categorySelectQuery = new Document("$in",str);
//		MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpBusinessAttributes");
//		 
//		FindIterable<Document> result = b_collection.find(new Document("categories",new Document("$all", str))).projection(fields(include("attribute"), excludeId()));
//		FindIterable<Document> result = b_collection.find(new Document("categories",new Document("$in", str)).append("attribute_keys", new Document("$in", str2))).projection(fields(include("name", "city", "stars", "state"), excludeId()));
//		MongoCollection<Document> b_collection1 =client.getDatabase("YelpApplication").getCollection("YelpBusiness");
//    	AggregateIterable<Document> list1 = b_collection1.aggregate(asList(match(eq("name", "Serenity Tattoo Removal")), 
//    																lookup("YelpReview", "business_id", "business_id", "YelpBusinessReviews"), 
//    																unwind("$YelpBusinessReviews"),
//    																lookup("YelpUser", "YelpBusinessReviews.user_id", "user_id", "UserDetails"),
//    																project(fields(computed("date", "$YelpBusinessReviews.date"),computed("business_id", "$business_id"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "$UserDetails.name"), computed("usefulVotes", "$YelpBusinessReviews.votes.useful"),  excludeId())),
//    																unwind("$userName")));
//    	try {
//			for(Document str1:list1){
//				System.out.println("list is: "+str1.toJson().toString().trim());
//   			   JSONParser parser = new JSONParser();
//               JSONObject jsonObject = (JSONObject) parser.parse(str1.toJson().toString().trim());
//               String date = (String) jsonObject.get("date");
//               System.out.println(date);
//               String stars = (String) jsonObject.get("stars").toString();
//               System.out.println(stars);
//               String text = (String) jsonObject.get("text");
//               String id = (String) jsonObject.get("userName");
//               String votesUseful=(String) jsonObject.get("usefulVotes").toString();
//               System.out.println(date + " " + stars + " " + text + " " + id + " " + votesUseful);
//			}
//		}
//   		catch(Exception e){
//   			System.out.println("Cant execute " + e);
//   			}
    	DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
    	DBCollection dbcollection=db1.getCollection("YelpUser");
    
    	
    	
    	
    	
    /*	db.YelpUser.aggregate([
    	                       { 
    	                         $match: {
    	                              $and: [ 
    	                                   
    	                                  {review_count: {$gt: 3}}, 
    	    							  {average_stars: {$gt: 3}}, 
    	                                  {yelping_since: {$gt:ISODate("2013-12-09T00:00:00Z")}},
    	    								new ISODate(2013-12-09T00:00:00Z)
    	    							  { "friends": { "$gte" : [ "$friends", "3" ] }}
    	    							   
    	                              ]
    	                         }
    	                       }
    	                      ])*/
    	
    	
    	String datestr="2014-12-09T00:00:00Z";
    	System.out.println("start"+"new ISODate("+datestr+")");
    	                      String option ="$and";
    	                     
    	 BasicDBObject proj1 = new BasicDBObject();
    	
    	String reviewcond = "$gte";
		BasicDBObject reviewcountcondDBOB = new BasicDBObject(reviewcond,3);
    	BasicDBObject reviewcountcond1 = new BasicDBObject("review_count",reviewcountcondDBOB);
    	
    	String starscond = "$gte";
		BasicDBObject averagestarscondDBOB = new BasicDBObject(starscond,3);
    	BasicDBObject averagestarscond1 = new BasicDBObject("average_stars",averagestarscondDBOB);    	
    	
    	String yelpingcond = "$gt";
		BasicDBObject yelpingsincecondDBOB = new BasicDBObject(yelpingcond, new Date ("01/01/2013"));//" ISODate("+datestr+")"
    	BasicDBObject yelpingsince = new BasicDBObject("yelping_since",yelpingsincecondDBOB); 
    	
    	//{ "friends": { "$gte" : [ "$friends", "3" ] }}
    	
    	BasicDBObject friendscondDOB = new BasicDBObject("$gte",asList("$friends","3"));
    	BasicDBObject friendsDOB = new BasicDBObject("friends",friendscondDOB);
    	
    	DBObject optioncond = new BasicDBObject(option ,asList(reviewcountcond1,averagestarscond1, yelpingsince,friendsDOB));           //reviewcountcond1,averagestarscond1,friendsDOB  ,yelpingsince 
    	       
    	
    	 
    	 
    	
    	DBObject match = new BasicDBObject("$match",optioncond);
    	System.out.println(match);
    	 DBObject project = new BasicDBObject("$project", new BasicDBObject("name", 1)
    	 .append("average_stars", 1).append("review_count", 1).append("yelping_since", 1).append("_id", 0));
    	
    	System.out.println(match );
    	 AggregationOutput output = dbcollection.aggregate(match,project);

    	 for (DBObject result : output.results()) {
    	  System.out.println(result);
    	  }
    	
    	
    	
    	
    		System.out.println("end");
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	BasicDBObject proj2 = new BasicDBObject();
    	q.put("categories", new BasicDBObject("$in", str));
   new BasicDBObject("adsad", "asdasd");
    	q.append("attribute_keys", new BasicDBObject("$in", str2));
    	proj1.put("name", 1);
    	proj1.append("city", 1);
    	proj1.append("stars", 1);
    	proj1.append("state", 1);
    	proj1.append("_id", 0);
   	
    	String pointOfInterested_Value = "4840 E Indian School Rd\nSte 101\nPhoenix, AZ 85018";
        	
    	BasicDBObject full = new BasicDBObject();
        full.append("full_address", pointOfInterested_Value);
        BasicDBObject proj=new BasicDBObject();
        proj.put("Loc", 1);
        DBCursor cursor = dbcollection.find(full,proj);
        DBObject res;
        double latitude = 0;
        double longitude = 0;
        while(cursor.hasNext()){
        	res=cursor.next();
        	System.out.println("cursor is:"+res);
        	DBObject report=(DBObject) res.get("Loc");
        	latitude=Double.parseDouble(report.get("latitude").toString().trim());
        	longitude=Double.parseDouble(report.get("longitude").toString().trim());
        	System.out.println("latitude" + latitude);
        	System.out.println("longitude" + longitude);
        }

        	BasicDBList geoCoord = new BasicDBList();
            geoCoord.add(longitude);
            geoCoord.add(latitude);

            BasicDBList geoParams = new BasicDBList();
            geoParams.add(geoCoord);
            geoParams.add( 1/3963.2);

            q.append("Loc", new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", geoParams)));
            System.out.println("FromHere");
            System.out.println(q);
            DBCursor sex = dbcollection.find(q,proj1);
    	       while(sex.hasNext()){
    	       	
    	       	System.out.println(sex.next().toString());
    	       }
//            }
//		System.out.println(result);
//		List<String> attributes = new ArrayList<String>();
//		list1.forEach(new Block<Document> (){
//        	@Override
//			public void apply(final Document document) {
//				// TODO Auto-generated method stub
//        		attributes.add(document.toJson().toString().trim());
//			}
//        	});
//        List<String> attributeList = new ArrayList<String>(new LinkedHashSet<String>(attributes));
//        for(String str1:attributeList){
//        	System.out.println(str1);
//        }
	}

	 
	 private static String condstr(String review_condition) {
			
			String cond;
			if (review_condition.equalsIgnoreCase(">"))
				cond = "$gte";
			else if (review_condition.equalsIgnoreCase("="))
				cond = "$eqe";
			else 
				cond = "$lte";
			
			if(true)System.out.println("Input = "+ review_condition+" output ="+cond);
			return cond;
		}
	public static List getReviewBusiness(Date reviewfrom ,Date Reviewto, String Stars_cond ,String Stars_value,String votes_cond,String votes_value){
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