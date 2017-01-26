/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import static java.sql.Types.CLOB;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Ganesha
 */
public class ParseJson {
	static Boolean debug = false;

	public void parseUser(Connection con, String filename) {
		try {
			FileReader reader = new FileReader("./" + filename);// yelp_user.json");
			BufferedReader br = new BufferedReader(reader);
			int count = 0;
			PreparedStatement statement;
			statement = con
					.prepareStatement("insert into Y_user_tB (Y_YELPING_SINCE, Y_REVIEW_COUNT, Y_NAME, Y_ID, Y_AVERAGE_STARS, Y_FRIENDS_COUNT)\n"
							+ "values(to_date(?,'yyyy-mm-dd'),?,?,?,?,?)");

			con.setAutoCommit(false);
			String line;
			while ((line = br.readLine()) != null) {
				if (debug == true)
					System.out.println(line);
				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(line);

				// Froom date
				String yelping_since = (String) jsonObject.get("yelping_since");
				yelping_since = yelping_since + "-01";

				String review_count = jsonObject.get("review_count").toString();

				String name = (String) jsonObject.get("name");
				String user_id = (String) jsonObject.get("user_id");

				String fans = (jsonObject.get("fans") != null) ? jsonObject
						.get("fans").toString() : "0";
				String average_stars = (jsonObject.get("average_stars") != null) ? jsonObject
						.get("average_stars").toString() : "0";

				JSONArray friends = (JSONArray) jsonObject.get("friends");
				int fcount = 0;
				if (friends != null)
					fcount = friends.size();
				if (debug == true)
					System.out.println("count of friends " + fcount);

				if (debug == true)
					System.out.println("setting parameters");
				statement.setNString(1, yelping_since);

				statement.setInt(2, Integer.parseInt(review_count));
				statement.setNString(3, name);
				statement.setNString(4, user_id);

				statement.setFloat(5, Float.parseFloat(average_stars));

				statement.setInt(6, fcount);
				statement.addBatch();
				if (debug == true)
					System.out.println("adding to batch");
				count++;

				if (count % 3000 == 2999) {
					statement.executeBatch();
					con.commit();
					statement.clearBatch();
				}
				if (debug == true)
					System.out.println("adding to batch end");
			}

			if (debug == true)
				System.out.println("executing in batch");
			statement.executeBatch();
			con.commit();
			statement.close();
			reader.close();
			br.close();

		} catch (FileNotFoundException ex) {
			System.out.println(ex.getMessage());
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ParseException | SQLException ex) {
			ex.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void parseBussiness(Connection con, String filename)
			throws Exception {

		Map bussiness_op = new HashMap<String, JSONObject>();
		Map bussiness_categories = new HashMap<String, JSONObject>();
		try {
			// FileReader fr = new FileReader("./yelp_business.json");

			FileReader fr = new FileReader("./" + filename);
			BufferedReader br = new BufferedReader(fr);
			int count = 0;
			PreparedStatement statement;
			statement = con
					.prepareStatement("insert into Y_BUSINESS_TB (b_bid, b_full_address, b_openStatus, b_city, b_review_count, b_Name, b_longitude, b_state, b_stars, b_latitude, b_Type)\n"
							+ "values(?,?,?,?,?,?,?,?,?,?,?)");
			con.setAutoCommit(false);
			String line;

			while ((line = br.readLine()) != null) {

				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(line);

				// Business ID (PK)
				String bid = (String) jsonObject.get("business_id");
				bussiness_op.put(bid, jsonObject.get("hours"));
				bussiness_categories.put(bid, jsonObject.get("categories"));
				String full_address = (String) jsonObject.get("full_address");
				String openStatus = (jsonObject.get("fans") != null) ? jsonObject
						.get("open").toString() : "false";
				String city = (String) jsonObject.get("city");
				String review_count = (jsonObject.get("review_count") != null) ? jsonObject
						.get("review_count").toString() : "0";
				String businessName = jsonObject.get("name").toString();
				String longitude = (jsonObject.get("longitude") != null) ? jsonObject
						.get("longitude").toString() : "";
				String state = (String) jsonObject.get("state");
				String stars = (jsonObject.get("stars") != null) ? jsonObject
						.get("stars").toString() : "0";
				String latitude = (jsonObject.get("latitude") != null) ? jsonObject
						.get("latitude").toString() : "";
				String bType = (String) jsonObject.get("type");

				statement.setNString(1, bid);
				statement.setNString(2, full_address);
				statement.setNString(3, openStatus);
				statement.setNString(4, city);
				statement.setInt(5, Integer.parseInt(review_count));
				statement.setNString(6, businessName);
				statement.setFloat(7, Float.parseFloat(longitude));
				statement.setNString(8, state);
				statement.setFloat(9, Float.parseFloat(stars));
				statement.setFloat(10, Float.parseFloat(latitude));
				statement.setNString(11, bType);
				statement.addBatch();
				count++;

				if (count % 3000 == 2999) {
					statement.executeBatch();
					con.commit();
					statement.clearBatch();
				}
			}
			statement.executeBatch();
			statement.clearBatch();
			con.commit();
			statement.close();
			// updating bussiness timings
			if (debug == true)
				System.out.println("bussiness_op + " + bussiness_op);
			yelpBusinessTimings(con, bussiness_op);
			Map bussiness_sub_categories = new HashMap<String, ArrayList>();
			bussiness_sub_categories = yelpBusinesscategories(con,
					bussiness_categories, bussiness_sub_categories);

			insertSubcategories(con, bussiness_sub_categories);
			fr.close();
			br.close();

		} catch (FileNotFoundException ex) {
			System.out.println(ex.getMessage());
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ParseException | SQLException ex) {
			ex.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	public Map yelpBusinesscategories(Connection con, Map bc, Map b_sc)
			throws SQLException, FileNotFoundException, IOException,
			ParseException {
		// Business categories parsing
		con.setAutoCommit(false);

		PreparedStatement satatement = con
				.prepareStatement("insert into Y_BUS_CTGRY_TB (b_id,BC_NAME) values(?,? )");

		int count = 0;
		String bid;
		Set bcset = bc.keySet();
		Iterator iterator = bcset.iterator();
		String mainCat = null;
		while (iterator.hasNext()) {
			bid = (String) iterator.next();

			JSONArray categories = (JSONArray) bc.get(bid);
			Iterator ca_iterator = categories.iterator();
			List subcatlist = null;

			subcatlist = new ArrayList();
			while (ca_iterator.hasNext()) {

				satatement.setNString(1, bid);
				String catname = ca_iterator.next().toString();
				if (debug == true)
					System.out.println(bid + " " + catname);
				// System.out.println(bid + " "+catname );
				satatement.setNString(2, catname);
				if ("Active Life".equalsIgnoreCase(catname)
						|| catname.equalsIgnoreCase("Arts & Entertainment")
						|| catname.equalsIgnoreCase("Automotive")
						|| catname.equalsIgnoreCase("Car Rental")
						|| catname.equalsIgnoreCase("Cafes")
						|| catname.equalsIgnoreCase("Beauty & Spas")
						|| catname.equalsIgnoreCase("Convenience Stores")
						|| catname.equalsIgnoreCase("Dentists")
						|| catname.equalsIgnoreCase("Doctors")
						|| catname.equalsIgnoreCase("Drugstores")
						|| catname.equalsIgnoreCase("Department Stores")
						|| catname.equalsIgnoreCase("Education")
						|| catname
								.equalsIgnoreCase("Event Planning & Services")
						|| catname.equalsIgnoreCase("Flowers & Gifts")
						|| catname.equalsIgnoreCase("Food")
						|| catname.equalsIgnoreCase("Health & Medical")
						|| catname.equalsIgnoreCase("Home Services")
						|| catname.equalsIgnoreCase("Home & Garden")
						|| catname.equalsIgnoreCase("Hospitals")
						|| catname.equalsIgnoreCase("Hotels & Travel")
						|| catname.equalsIgnoreCase("Hardware Stores")
						|| catname.equalsIgnoreCase("Grocery")
						|| catname.equalsIgnoreCase("Medical Centers")
						|| catname.equalsIgnoreCase("Nurseries & Gardening")
						|| catname.equalsIgnoreCase("Nightlife")
						|| catname.equalsIgnoreCase("Restaurants")
						|| catname.equalsIgnoreCase("Shopping")
						|| catname.equalsIgnoreCase("Transportation")) {

					mainCat = catname;
					System.out.println(bid + " mainCat" + mainCat);
				} else {
					subcatlist.add(catname);
					System.out.println(bid + " catname" + catname);

				}

				// System.out.println(user_id + " " + iterator.next());
				satatement.addBatch();
				count++;
			}

			if (count % 3000 == 2999) {
				satatement.executeBatch();
				satatement.clearBatch();
			}

			// Add sub categories

			try {

				b_sc.put(bid + "#" + mainCat, subcatlist);
				// insertSubcategories(con, bid,mainCat,categories);
			} finally {
				// conn
				// System.out.println(" in finnaly "+mainCat);
			}
			if (debug == true)
				System.out.println(count + "\n");
		}
		satatement.executeBatch();
		satatement.clearBatch();
		con.commit();
		satatement.close();
		return b_sc;
	}

	private void insertSubcategories(Connection con, Map bscat)
			throws SQLException, FileNotFoundException, IOException,
			ParseException {

		// String bid,String maincat,JSONArray categories

		PreparedStatement sbca_satatement = con
				.prepareStatement("insert into Y_BUS_SUB_CATEGORY (b_id,S_BUSINESS_CATAGEORY,S_SUB_CAT_NAME) values(?,? ,?)");
		Set bcset = bscat.keySet();

		int count = 0;
		Iterator iteratormap = bcset.iterator();
		while (iteratormap.hasNext()) {
			String keymap = (String) iteratormap.next();
			String[] parts = keymap.split("#");
			ArrayList categories = (ArrayList) bscat.get(keymap);
			Iterator subca_iterator = categories.iterator();
			while (subca_iterator.hasNext()) {
				sbca_satatement.setNString(1, parts[0]);
				String sub_catname = subca_iterator.next().toString();
				if (debug == true)
					System.out.println(parts[0] + "parts[1]" + parts[1] + " "
							+ sub_catname);
				System.out.println(parts[0] + "parts[1]" + parts[1] + " "
						+ sub_catname);
				sbca_satatement.setNString(2, parts[1]);
				sbca_satatement.setNString(3, sub_catname);
				// sbca_satatement.executeUpdate();
				sbca_satatement.addBatch();
				count++;
			}

			if (count % 3000 == 2999) {
				sbca_satatement.executeBatch();
				sbca_satatement.clearBatch();
			}
		}
		sbca_satatement.executeBatch();
		sbca_satatement.clearBatch();
		sbca_satatement.close();

	}

	public void yelpBusinessTimings(Connection con, Map bussiness_op)
			throws SQLException, FileNotFoundException, IOException,
			ParseException {
		// Business Timings parsing
		con.setAutoCommit(false);

		PreparedStatement satatement = con
				.prepareStatement("insert into Y_B_DAYSOFOPERATION_TB values(?, to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'),to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'), to_date(?,'HH24:MI:SS'))");

		int count = 0;
		String bid;
		Set buss_timeset = bussiness_op.keySet();
		Iterator iterator = buss_timeset.iterator();

		while (iterator.hasNext()) {
			bid = (String) iterator.next();

			// Get Individual objects for every day
			if (debug == true)
				System.out.println(bussiness_op.get(bid));
			JSONObject hours = (JSONObject) bussiness_op.get(bid);
			JSONObject Monday = (JSONObject) hours.get("Monday");
			JSONObject Tuesday = (JSONObject) hours.get("Tuesday");
			JSONObject Wednesday = (JSONObject) hours.get("Wednesday");
			JSONObject Thursday = (JSONObject) hours.get("Thursday");
			JSONObject Friday = (JSONObject) hours.get("Friday");
			JSONObject Saturday = (JSONObject) hours.get("Saturday");
			JSONObject Sunday = (JSONObject) hours.get("Sunday");

			String MondayOpen = (String) (Monday == null ? "" : Monday
					.get("open"));
			String MondayClose = (String) (Monday == null ? "" : Monday
					.get("close"));
			String TuesdayOpen = (String) (Tuesday == null ? "" : Tuesday
					.get("open"));
			String TuesdayClose = (String) (Tuesday == null ? "" : Tuesday
					.get("close"));
			String WednesdayOpen = (String) (Wednesday == null ? "" : Wednesday
					.get("open"));
			String WednesdayClose = (String) (Wednesday == null ? ""
					: Wednesday.get("close"));
			String ThursdayOpen = (String) (Thursday == null ? "" : Thursday
					.get("open"));
			String ThursdayClose = (String) (Thursday == null ? "" : Thursday
					.get("close"));
			String FridayOpen = (String) (Friday == null ? "" : Friday
					.get("open"));
			String FridayClose = (String) (Friday == null ? "" : Friday
					.get("close"));
			String SaturdayOpen = (String) (Saturday == null ? "" : Saturday
					.get("open"));
			String SaturdayClose = (String) (Saturday == null ? "" : Saturday
					.get("close"));
			String SundayOpen = (String) (Sunday == null ? "" : Sunday
					.get("open"));
			String SundayClose = (String) (Sunday == null ? "" : Sunday
					.get("close"));

			satatement.setString(1, bid);
			satatement.setString(2, MondayOpen);
			satatement.setString(3, MondayClose);
			satatement.setString(4, TuesdayOpen);
			satatement.setString(5, TuesdayClose);
			satatement.setString(6, WednesdayOpen);
			satatement.setString(7, WednesdayClose);
			satatement.setString(8, ThursdayOpen);
			satatement.setString(9, ThursdayClose);
			satatement.setString(10, FridayOpen);
			satatement.setString(11, FridayClose);
			satatement.setString(12, SaturdayOpen);
			satatement.setString(13, SaturdayClose);
			satatement.setString(14, SundayOpen);
			satatement.setString(15, SundayClose);

			satatement.addBatch();

			if (++count % 100 == 0) {
				satatement.executeBatch();
				satatement.clearBatch();
				con.commit();
			}
			if (debug == true)
				System.out.println(count + "\n");
		}
		satatement.executeBatch();
		satatement.clearBatch();
		con.commit();
		satatement.close();

	}

	public void parseReviews(Connection con, String filename) {

		Map bussiness_op = new HashMap<String, JSONObject>();
		Map bussiness_categories = new HashMap<String, JSONObject>();
		try {
			// FileReader fr = new FileReader("./yelp_business.json");

			FileReader fr = new FileReader("./" + filename);
			// FileReader fr = new FileReader("./yelp_review.json");
			BufferedReader br = new BufferedReader(fr);
			int count = 0;
			String file_line;
			con.setAutoCommit(false);
			PreparedStatement p_statement;
			p_statement = con
					.prepareStatement("INSERT INTO Y_REVIEWS_TB(R_REVIEW_ID, R_AUTHOR, R_BUSS_ID, R_VOTES, R_STARS, R_PUBLISH_DATE, R_REVIEW_TEXT) \n"
							+ "VALUES(?, ?, ?,  ?, ?, to_date(?,'yyyy-mm-dd'), ? )");
			while ((file_line = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(file_line);

				// Votes Parsing
				JSONObject votes = (JSONObject) jsonObject.get("votes");
				String funny = votes.get("funny").toString();
				String useful = votes.get("useful").toString();
				String cool = votes.get("cool").toString();
				int votes_all = Integer.parseInt(funny)
						+ Integer.parseInt(useful) + Integer.parseInt(cool);
				String review_id = (String) jsonObject.get("review_id");
				String user_id = (String) jsonObject.get("user_id");
				String stars = (String) jsonObject.get("stars").toString();
				String date = (String) jsonObject.get("date");
				String text = (String) jsonObject.get("text");

				String business_id = (String) jsonObject.get("business_id");

				p_statement.setNString(1, review_id);
				p_statement.setNString(2, user_id);
				p_statement.setNString(3, business_id);
				p_statement.setInt(4, votes_all);
				p_statement.setFloat(5, Float.parseFloat(stars));
				p_statement.setNString(6, date);
				StringReader stringReader = new StringReader(text);
				p_statement.setCharacterStream(7, stringReader, text.length());

				p_statement.addBatch();
				// p_statement.executeUpdate();
				// p_statement.close();
				if (++count % 3000 == 0) {
					p_statement.executeBatch();
					p_statement.clearBatch();
					con.commit();
					if (debug == true)
						System.out.println(count + "\n");
				}
				// count++;
				// System.out.println(count+"\n");
			}
			p_statement.executeBatch();
			p_statement.clearBatch();
			p_statement.close();
			con.commit();
			br.close();
			fr.close();
			// count =1;
		} catch (IOException ex) {
			System.out.println(ex);
		} catch (ParseException ex) {
			System.out.println(ex);
		} catch (NullPointerException ex) {
			System.out.println(ex);
		} catch (SQLException ex) {
			System.out.println(ex);
		}

	}

	public void parseCheckin(Connection con, String filename) throws Exception {

		// Map bussiness_op = new HashMap<String,JSONObject>();
		// Map bussiness_categories = new HashMap<String,JSONObject>();
		try {
			// FileReader fr = new FileReader("./yelp_business.json");

			FileReader fr = new FileReader("./" + filename);
			// FileReader fr = new FileReader("./yelp_review.json");
			BufferedReader br = new BufferedReader(fr);
			int count = 0;
			String file_line;
			con.setAutoCommit(false);
			PreparedStatement p_statement;
			p_statement = con
					.prepareStatement("INSERT INTO Y_CHECKIN_TB(C_B_ID, C_info,c_count) \n"
							+ "VALUES(?, ? ,?)");
			while ((file_line = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(file_line);

				// Votes Parsing
				String business_id = (String) jsonObject.get("business_id");
				JSONObject checkininfo = (JSONObject) jsonObject
						.get("checkin_info");
				String[][] checkin = new String[24][7];
				String[][] checkinT = new String[7][24];
				int checkincount = 0;
				String c_info = null;
				for (int i = 0; i < 24; i++) {
					for (int j = 0; j < 7; j++) {
						checkin[i][j] = getcheckininfo(checkininfo, i + "-" + j);
						c_info = getcheckininfo(checkininfo, i + "-" + j);
						if (c_info != null) {
							p_statement.setNString(1, business_id);
							String dayhour = (i >= 10) ? "" + j + i : j + "0"
									+ i;
							p_statement.setNString(2, dayhour);
							p_statement.setNString(3, c_info);// count
							checkincount++;
							p_statement.addBatch();
							// make changes to checkin to hold day and then
							// hours
							// also store the count.

							if (debug == true)
								System.out.println(" i = " + i + "J = " + j
										+ "Value " + c_info);
						}

						// System.out.println("checkin [][]= "+i+" "+j+" "+checkin[i][j]);
					}
				}
				// System.out.println("checkin [][]= "+checkin);

				// checkinT = ParseJson.transpose(checkin);

				p_statement.addBatch();
				// p_statement.executeUpdate();
				// p_statement.close();
				if (++count % 3000 == 0) {
					p_statement.executeBatch();
					p_statement.clearBatch();
					con.commit();
					if (debug == true)
						System.out.println(count + "\n");
				}
				// count++;
				// System.out.println(count+"\n");
			}
			p_statement.executeBatch();
			p_statement.clearBatch();
			p_statement.close();
			con.commit();
			br.close();
			fr.close();
			// count =1;
		} catch (IOException ex) {
			System.out.println(ex);
		} catch (ParseException ex) {
			System.out.println(ex);
		} catch (NullPointerException ex) {
			System.out.println(ex);
		} catch (SQLException ex) {
			System.out.println(ex);
		} catch (Exception ex) {
			System.out.println(ex);
			ex.printStackTrace();
			throw ex;
		}

	}

	String getcheckininfo(JSONObject obj, String index) {
		String t0_1 = null;
		if (obj.get(index) != null)
			t0_1 = obj.get(index).toString();
		return t0_1;
	}

	public static String[][] transpose(String[][] m) {
		String[][] temp = new String[m[0].length][m.length];
		for (int i = 0; i < m.length; i++)
			for (int j = 0; j < m[0].length; j++)
				temp[j][i] = m[i][j];

		return temp;

	}
}
