/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

import java.sql.*;

public class YelpDBConnectionFactory {

	private static Connection con = null;

	public static Connection connectDatabase() {
		String DriverClassName = "oracle.jdbc.OracleDriver";
		String url = "jdbc:oracle:thin:@localhost:1521:orcl";
		String userName = "scott";
		String pwd = "tiger";
		try {
			Class.forName(DriverClassName);
			con = DriverManager.getConnection(url, userName, pwd);
			if (con != null) {
				System.out.println("Connection Established !!");
			}
		} catch (ClassNotFoundException e) {
			System.out.println("Class Not found");
		} catch (SQLSyntaxErrorException e) {
			System.out.println("Table not found " + e);
		} catch (SQLException e) {
			System.out.println("Connection Not established" + e);
		} catch (Exception e) {

		}
		return con;
	}

	public static void main(String[] args) {

		Connection dbConnection = YelpDBConnectionFactory.connectDatabase();

		if (dbConnection != null) {

			// GUI g = new GUI();
			// g.invokeApplication();

			try {

				// JsonParsing jp = new JsonParsing();
				// con.setAutoCommit(false);
				// jp.YelpUserParse(dbConnection);
				// jp.YelpFriends(dbConnection);
				// jp.userelite(dbConnection);
				// jp.yelpBusiness(dbConnection);
				// jp.yelpBusinessTimings(dbConnection);
				// jp.yelpBusinessCategory(dbConnection);
				// jp.yelpBusinessAttributes(dbConnection);
				// jp.Yelp_review(dbConnection);

				// String query = "select distinct d.categoryname \n"
				// + "from businesscategories d \n"
				// +
				// "join(select business_id from businesscategories  where categoryname = 'Restaurants') b on d.business_id = b.business_id \n"
				// + "order by d.categoryname";
				// Statement statement = dbConnection.createStatement();
				// ResultSet resultSet = statement.executeQuery(query);
				// while(resultSet.next())
				// {
				// //System.out.println(resultSet.getString(resultSet.findColumn("REVIEWID"))
				// + " " +
				// resultSet.getString(resultSet.findColumn("VOTES_FUNNY")));
				// }

			}

			catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

}
