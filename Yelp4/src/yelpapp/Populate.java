/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Populate {

	static Connection dbconnection = YelpDBConnectionFactory.connectDatabase();

	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.out
					.println("Usage: java populate yelp_business.json yelp_review.json yelp_checkin.json yelp_user.json");
		}
		for (String s : args) {
			System.out.println(s);
		}
		try {
			Boolean test = false;
			// Populate.deleterowsfromTables(dbconnection);
			String userfile = args[3];
			String bussinessfile = args[0];
			String reviewfile = args[1];
			String checkinfile = args[2];
			if (test) {
				userfile = "yelp_user_test.json";
				bussinessfile = "yelp_business_test.json";

				reviewfile = "yelp_review_test.json";
				checkinfile = "yelp_checkin_test.json";
			}
			ParseJson parse = new ParseJson();
			System.out.println("processing file " + userfile);
			parse.parseUser(dbconnection, userfile);
			System.out.println("processing file " + bussinessfile);
			parse.parseBussiness(dbconnection, bussinessfile);
			System.out.println("processing file " + reviewfile);
			parse.parseReviews(dbconnection, reviewfile);
			parse.parseCheckin(dbconnection, checkinfile);
			System.out.println("end of processing processing fils ");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			if (dbconnection != null) {
				dbconnection.close();
			}

		}

	}

	public static void deleterowsfromTables(Connection dbConnection)
			throws SQLException {

		Statement statement = null;

		String delete_y_user_SQL = "delete from  Y_USER_TB ";
		String delete_Y_BUSINESS_TB_SQL = "delete from  Y_BUSINESS_TB ";
		// String delete_Y_REVIEWS_SQL = "DELETE Y_REVIEWS_TB ";

		try {
			dbConnection = YelpDBConnectionFactory.connectDatabase();
			dbConnection.setAutoCommit(false);
			statement = dbConnection.createStatement();
			dbConnection.commit();
			System.out.println("delete_y_user_SQL : Records are deleted!");
			// statement = dbConnection.prepareStatement(
			// "delete from   Y_CHECKIN_TB");
			int count = statement.executeUpdate("delete from   Y_CHECKIN_TB");
			dbConnection.commit();

			System.out.println("delete_y_user_SQL : Records are deleted!="
					+ count);
			// statement = dbConnection.prepareStatement(
			// "delete from  Y_REVIEWS_TB");
			count = statement.executeUpdate("delete from  Y_REVIEWS_TB");
			dbConnection.commit();
			System.out.println("Y_REVIEWS_TB : Records are deleted!" + count);

			// execute delete SQL stetement
			count = statement.executeUpdate(delete_y_user_SQL);
			dbConnection.commit();
			System.out.println("delete_y_user_SQL : Records are deleted! "
					+ count);
			count = statement
					.executeUpdate("delete from  Y_B_DAYSOFOPERATION_TB");
			dbConnection.commit();
			System.out.println("Y_B_DAYSOFOPERATION_TB : Records are deleted! "
					+ count);
			dbConnection.commit();
			count = statement.executeUpdate("delete from  Y_BUS_CTGRY_TB");

			dbConnection.commit();
			count = statement.executeUpdate(delete_Y_BUSINESS_TB_SQL);

			System.out
					.println("delete_Y_BUSINESS_TB_SQL : Records are deleted!"
							+ count);
			dbConnection.commit();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			throw e;
		} finally {

			if (statement != null) {
				statement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}

}
