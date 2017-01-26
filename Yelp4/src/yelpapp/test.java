/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Ganesha
 */
public class test {

	public static void main(String args[]) {

		DateFormat df = new SimpleDateFormat("yyyy-MMM-dd");
		java.util.Date today = Calendar.getInstance().getTime();

		String reportDate = df.format(today);

		// Print what date is today!
		System.out.println("Report Date: " + reportDate);

		HW3 asd = new HW3();
		today = new Date("01-01/2014");
		// SearchResults asd1 = asd.getUserDetails(today, "=" ,
		// "5",">","4",">","3","and" );
		// System.out.println(""+asd1.getUserSearchresults());

		int fromhour = 17;
		int fromday = 6;
		int tohour = 6;
		int to1day = 2;

		// is between 6-17 and 6-23
		// and i%7 till to1day and between 1-0 to 1-6
		String query = " C_info between '" + fromhour + "-" + fromday
				+ "' and '23-" + fromday + "' and ";
		for (int i = (fromday + 1) % 7; i < to1day; i++) {
			query = query + " C_info like '__-" + i + "' and ";
		}
		query = query + "C_info between '0-" + to1day + "' and '" + tohour
				+ "-" + to1day + "'";
		System.out.println("query" + query);

		ArrayList a = new ArrayList();
		a.add("adasd");
		a.add("asdasdadsasd");
		System.out.println(asd.formincondition(a));

	}

}
