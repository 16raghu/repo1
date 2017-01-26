package yelpapp;

import java.sql.Connection;

import org.bson.Document;
import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoUtilites {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Begin");
		addLocAttribute();
		System.out.println("done");
		// db.YelpBusiness.createIndex( { loc : "2dsphere" } )

	}

	public static void addLocAttribute() {

		Connection dbconnection;
		// dbconnection = YelpDBConnectionFactory.connectDatabase();
		// DB db1asd = new
		// MongoClient("localhost",27017).getDB("YelpApplication");
		// DBCollection dbcollection=db1asd.getCollection("YelpBusiness");
		/* mongo = new MongoClient("localhost", 27017); */
		MongoDatabase db1 = new MongoClient("localhost", 27017)
				.getDatabase("YelpApplication");
		MongoCollection<Document> zips = db1.getCollection("YelpBusiness");// db.getCollection("yelp_business");
		// BasicBSONObject bSONObject= new BasicBSONObject();
		// bSONObject.append("categories", "categories");
		// BsonString bsonString= new BsonString("categories");
		// BsonDocument bson = new BsonDocument("categories", bsonString);
		// bson.put("categories", new BasicDBObject("$in",
		// Selected_listCategories));
		FindIterable<Document> result = zips.find();
		MongoCursor<Document> iterator = result.iterator();
		while (iterator.hasNext()) {
			Document doc = iterator.next();
			Double longitude = doc.getDouble("longitude");
			Double latitude = doc.getDouble("latitude");
			System.out.println(longitude + "    " + latitude);
			double[] loc = { longitude, latitude };
			BasicBSONList bSONList = new BasicBSONList();
			bSONList.add(longitude);
			bSONList.add(latitude);
			BasicDBObject location = new BasicDBObject("loc", loc);
			BasicDBObject set = new BasicDBObject("$set", location);

			zips.updateOne(new BasicDBObject("_id", doc.get("_id")), set);

		}
	}

}
