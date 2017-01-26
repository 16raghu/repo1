/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpapp;

import java.util.List;

/**
 *
 * @author Ganesha
 */
public class SearchResults {

	int numberofrecords;
	String Query;
	String jason;

	public String getJason() {
		return jason;
	}

	public void setJason(String jason) {
		this.jason = jason;
	}

	List<String> userStr;

	public int getNumberofrecords() {
		return numberofrecords;
	}

	public void setNumberofrecords(int numberofrecords) {
		this.numberofrecords = numberofrecords;
	}

	public List<String> getUserStr() {
		return userStr;
	}

	public void setUserStr(List<String> userStr) {
		this.userStr = userStr;
	}

	List UserSearchresults;

	List BusinessSearchResults;
	Boolean isusersearchResults;

	public void setUserSearchresults(List UserSearchresults) {
		this.UserSearchresults = UserSearchresults;
	}

	public void setBusinessSearchResults(List BusinessSearchResults) {
		this.BusinessSearchResults = BusinessSearchResults;
	}

	public void setIsusersearchResults(Boolean isusersearchResults) {
		this.isusersearchResults = isusersearchResults;
	}

	public List getUserSearchresults() {
		return UserSearchresults;
	}

	public List getBusinessSearchResults() {
		return BusinessSearchResults;
	}

	public Boolean getIsusersearchResults() {
		return isusersearchResults;
	}

	public String getQuery() {
		return Query;
	}

	public void setQuery(String Query) {
		this.Query = Query;
	}

}
