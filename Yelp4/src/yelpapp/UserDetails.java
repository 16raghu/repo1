/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

/**
 *
 * @author Ganesha
 */
public class UserDetails {

	String yname;
	String Yelp_Since;
	String average_stars;
	String Friends_count;
	String yid;

	@Override
	public String toString() {
		return "UserDetails{" + "yname=" + yname + ", Yelp_Since=" + Yelp_Since
				+ ", average_stars=" + average_stars + ", Friends_count="
				+ Friends_count + ", yid=" + yid + '}';
	}

	public String getYid() {
		return yid;
	}

	public void setYid(String yid) {
		this.yid = yid;
	}

	public String getYname() {
		return yname;
	}

	public void setYname(String yname) {
		this.yname = yname;
	}

	public String getYelp_Since() {
		return Yelp_Since;
	}

	public void setYelp_Since(String Yelp_Since) {
		this.Yelp_Since = Yelp_Since;
	}

	public String getAverage_stars() {
		return average_stars;
	}

	public void setAverage_stars(String average_stars) {
		this.average_stars = average_stars;
	}

	public String getFriends_count() {
		return Friends_count;
	}

	public void setFriends_count(String Friends_count) {
		this.Friends_count = Friends_count;
	}

}
