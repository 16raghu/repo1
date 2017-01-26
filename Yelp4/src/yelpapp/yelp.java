package yelpapp;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static java.util.Arrays.asList;

import java.awt.Color;
import java.awt.EventQueue;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.UIManager;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableModel;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;

public class yelp extends JFrame{
	  private static MongoClient client;
	private JFrame frame;
	private JTable resulttable;
	private JTextField numberofcheckValues;
	private JTextField Fromdate_textField;
	private JTextField todatetextField;
	private JTextField Stars_value;
	private JTextField votesValue;
	private DefaultTableModel tableList;
	private JTextField memberSinceValue;
	private JTextField reviewcountValue;
	private JTextField NumofFriendsValue;
	private JTextField AvgstarsValue;
	private JTable table_review;
	JComboBox reviewstarsCond;
	JComboBox noofcheckincond;
	private JScrollPane Review_pane;
	JComboBox friends_cond = new JComboBox();
	JComboBox avgstarscond = new JComboBox();
	JComboBox maincond = new JComboBox();
	JComboBox reviewvotesCond;
	JComboBox reviewcountcond;
	JComboBox fromday;
	JComboBox fromhour;
	JComboBox checkintohour;
	JTextPane queryPane;
	JComboBox checkin_todate;
	JComboBox userVotesCond;
	  JComboBox proximityComboBox;
	    JComboBox searchForComboBox;
	    Map Addressmap = new HashMap();
	private CatPanelCheckBoxActionHandler catPanelCheckBoxActionHandler;
	private CatPanelCheckBoxActionHandler SubcatPanelCheckBoxActionHandler;
	JPanel MainCategoryPanel;
	private JPanel SubCategoryPanel;
	JScrollPane resultPane;
	JRadioButton rdbtnBusinessSearch;
	JRadioButton rdbtnUserSearch;
	JComboBox pointOfInterestComboBox;
	 JTable reviewTable ;
	    JScrollPane scrollPane_review;
	    private JTextField uservotesValue;
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					yelp window = new yelp();
					window.frame.setVisible(true);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public yelp() {
		initialize();
		 
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 886, 569);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		JPanel checkinpanel = new JPanel();
		checkinpanel.setName("Checkin Criteria");
		checkinpanel.setBorder(new TitledBorder(null, "Checkin", TitledBorder.LEADING, TitledBorder.TOP, null, Color.GRAY));
		
		JPanel reviewpanel = new JPanel();
		reviewpanel.setBorder(new TitledBorder(null, "Review", TitledBorder.LEADING, TitledBorder.TOP, null, Color.GRAY));
		
		 resultPane = new JScrollPane();
		
		JPanel userpanel = new JPanel();
		userpanel.setBorder(new TitledBorder(null, "User Criteria", TitledBorder.LEADING, TitledBorder.TOP, null, null));
		
		JButton btnNewButton = new JButton("Execute Query");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				
				HW3 hw3=new HW3();
				
					String memberScince = memberSinceValue.getText();
					System.out.println(memberScince);
				Date 	y_membersince;
				String 	review_condition;
				String 	review_value;
				String 	noofFriends_cond;
				String 	nooffriendsvalue;
				//String 	avgstarscond;
				String 	avgstarsVal;
				String 	maincondition;
				System.out.println("Radiobutton usersearch"+rdbtnUserSearch.isSelected());
				System.out.println("Radiobutton businesssearch"+rdbtnBusinessSearch.isSelected());
				if ((rdbtnUserSearch.isSelected()) ||  (!memberScince.equalsIgnoreCase("MM/DD/YYYY") && !memberScince.equalsIgnoreCase(""))  ){
				/*	 friends_cond.getSelectedItem().toString()
				avgstarscond.getSelectedItem().toString()
				maincond.getSelectedItem().toString()*/
					
					  y_membersince=StringtoDate(memberScince);
					  
					  SearchResults userseachresults = hw3.getUserDetailsQuery(y_membersince, reviewcountcond.getSelectedItem().toString(), reviewcountValue.getText(), friends_cond.getSelectedItem().toString(), NumofFriendsValue.getText(), avgstarscond.getSelectedItem().toString(), AvgstarsValue.getText(), maincond.getSelectedItem().toString(),userVotesCond.getSelectedItem().toString(),uservotesValue.getText());
					
					try {
						queryPane.setText(userseachresults.Query);
						
						tableList = new DefaultTableModel(new String[]{"Name", "Average stars","Yelping since","Review count"},0);
						resulttable.setModel(tableList);
						resulttable.revalidate();
						resulttable.repaint();
						
			        		for(String str:userseachresults.getUserStr()){
			        			JSONParser parser = new JSONParser();
			                    JSONObject jsonObject = (JSONObject) parser.parse(str);
			                    String Name = (String) jsonObject.get("name");
			                    Double stars = (Double) (jsonObject.get("average_stars"));
			                    JSONObject js1 = (JSONObject)jsonObject.get("yelping_since");
								String scince = (String) js1.get("$date");
			                    Long rcount = (Long) jsonObject.get("review_count");
			                   
			                    //populateReview(b_id);
			                    tableList.addRow(new Object[]{Name,stars,scince,rcount});
			                    
				        		
			        		}
			        		
			        		resulttable.addMouseListener(new MouseListener(){

			        			@Override
			        			public void mouseClicked(MouseEvent e) {
			        				// TODO Auto-generated method stub
			        				
			        				System.out.println("in mouse event");
			        				int row = resulttable.rowAtPoint(e.getPoint());
			        				String user_name = resulttable.getModel().getValueAt(row, 0).toString();
			        				if (row>=0)
			        				{
			        					populateuserreview(user_name);
			        				}
			        			}

			        			@Override
			        			public void mouseEntered(MouseEvent arg0) {
			        				// TODO Auto-generated method stub
			        				
			        			}

			        			@Override
			        			public void mouseExited(MouseEvent arg0) {
			        				// TODO Auto-generated method stub
			        				
			        			}

			        			@Override
			        			public void mousePressed(MouseEvent arg0) {
			        				// TODO Auto-generated method stub
			        				
			        			}

			        			@Override
			        			public void mouseReleased(MouseEvent arg0) {
			        				// TODO Auto-generated method stub
			        				
			        			}
			                	
			                });

		        		} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					 System.out.println("user done ");
				}
				else {
					
					
					
					
					ArrayList Cat = catPanelCheckBoxActionHandler.getCategoriesSelected();
					
					String checkinfrom= "011" ;
					String checkinto ="100";
					
					try {
						checkinfrom= fromday.getSelectedItem().toString().substring(0, 1)+fromhour.getSelectedItem().toString();
						checkinto= checkin_todate.getSelectedItem().toString().substring(0, 1)+checkintohour.getSelectedItem().toString();
						
						System.out.println(checkinfrom+checkinto +"asdasd");
					}catch(Exception e)
					{
						System.out.println(e.getMessage());
					}
					String noofcheckin_cond=noofcheckincond.getSelectedItem().toString();
					String noofcheckinvalue=numberofcheckValues.getText();
					Date reviewfrom = StringtoDate(Fromdate_textField.getText());
					System.out.println("reviewfrom"+reviewfrom);
					Date Reviewto =StringtoDate(todatetextField.getText());
					String Stars_cond =reviewstarsCond.getSelectedItem().toString();
					String Stars_value1 = Stars_value.getText();
					String votes_cond =reviewvotesCond.getSelectedItem().toString();
					String votes_value =votesValue.getText();
					String address=    (String)pointOfInterestComboBox.getSelectedItem();
					AddressClass addresas = (AddressClass)Addressmap.get(address);
					
					String q1 = queryCategoryQuery(Cat);
					
					Boolean isaddressselected =true;
					if (addresas == null)
						isaddressselected = false;
					Double longitude = 0d;
					Double latitude = 0d;
					Boolean isproximityset = true;
					String proximity = (String)proximityComboBox.getSelectedItem();
					 if (proximity.equals("None")){
						 isproximityset=false;
					 }
					if(isaddressselected) {
						longitude = addresas.Lonlongitude;
						latitude= addresas.Latitude;
						System.out.println("longitude "+longitude +latitude);
						if (isproximityset) 
							q1 = q1+","+ proxqueryonly(longitude, latitude, Integer.parseInt(proximity));
					}
					
					List <String> businessCat =hw3.getBusinessDetails(Cat, checkinfrom, checkinto, noofcheckin_cond, noofcheckinvalue, reviewfrom, Reviewto, Stars_cond, Stars_value1, votes_cond, votes_value,searchForComboBox.getSelectedItem().toString(),isaddressselected,longitude,latitude,isproximityset,proximity);
					
					
					System.out.println("Returned from business search");
	        		tableList = new DefaultTableModel(new String[]{"Name", "City","State","Stars"},0);
	        		resulttable.setModel(tableList);
	        		
	                 /*if(pointOfInterestComboBox.getSelectedItem().toString()=="NONE" || proximityComboBox.getSelectedItem().toString() == "NONE")
	                 {
	                	 BusinessSet = PopulateBusinessOnSearch();
	                 }
	                 else
	                 {
	                	 BusinessSet = PopulateBusinessesProximityOnSearch();
	                 }*/
	        		
	        		try {
	        			queryPane.setText(q1);
		        		for(String str:businessCat){
		        			JSONParser parser = new JSONParser();
		                    JSONObject jsonObject = (JSONObject) parser.parse(str);
		                    String Name = (String) jsonObject.get("name");
		                    String city = (String) jsonObject.get("city");
		                    String state = (String) jsonObject.get("state");
		                    Double stars = (Double) jsonObject.get("stars");
		                    //b_id=(String)jsonObject.get("business_id");
		                    //populateReview(b_id);
		                    tableList.addRow(new Object[]{Name,city,state,stars});
		                  
		        		}
	        		} catch ( Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	
	        	        resulttable.addMouseListener(new MouseListener(){

	        				@Override
	        				public void mouseClicked(MouseEvent e) {
	        					// TODO Auto-generated method stub
	        					int row = resulttable.rowAtPoint(e.getPoint());
	        					String business_name = resulttable.getModel().getValueAt(row, 0).toString();
	        					if (row>=0)
	        					{
	        						populateReview(business_name);
	        					}
	        				}

	        				@Override
	        				public void mouseEntered(MouseEvent arg0) {
	        					// TODO Auto-generated method stub
	        					
	        				}

	        				@Override
	        				public void mouseExited(MouseEvent arg0) {
	        					// TODO Auto-generated method stub
	        					
	        				}

	        				@Override
	        				public void mousePressed(MouseEvent arg0) {
	        					// TODO Auto-generated method stub
	        					
	        				}

	        				@Override
	        				public void mouseReleased(MouseEvent arg0) {
	        					// TODO Auto-generated method stub
	        					
	        				}
	        	        	
	        	        });
	     	
				}
				System.out.println(reviewcountValue.getText());
				System.out.println(NumofFriendsValue.getText());
				System.out.println(AvgstarsValue.getText());
			}

			private Date StringtoDate(String memberScince) {
				String startDateString =memberScince;
				DateFormat df = new SimpleDateFormat("MM/dd/yyyy"); 
				Date startDate = null;
				try {
				    startDate = df.parse(startDateString);
				    String newDateString = df.format(startDate);
				    System.out.println(newDateString);
				} catch (ParseException e) {
				    //System.out.println(e.getMessage());
					System.out.println(e.getMessage());
				}
				return startDate;
			}
			
		});
		
		
		
		JLabel memberSince = new JLabel("Member Since");
		
		memberSinceValue = new JTextField();
		memberSinceValue.setText("MM/DD/YYYY");
		memberSinceValue.setColumns(10);
		
		JLabel lblReviewCount = new JLabel("Review Count");
		
		JLabel lblNoofFriends = new JLabel("No.of Friends");
		
		JLabel lblAve = new JLabel("Avg. Stars");
		
		 reviewcountcond = new JComboBox();
		reviewcountcond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
		 friends_cond = new JComboBox();
		 avgstarscond = new JComboBox();
		 maincond = new JComboBox();
		friends_cond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
	
		avgstarscond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
		JLabel lblSelect = new JLabel("Select");
		
	
		maincond.setModel(new DefaultComboBoxModel(new String[] {"and, or", " and ", " or "}));
		
		JLabel label_3 = new JLabel("Value");
		
		reviewcountValue = new JTextField();
		reviewcountValue.setText("0");
		reviewcountValue.setColumns(10);
		
		JLabel label_4 = new JLabel("Value");
		
		NumofFriendsValue = new JTextField();
		NumofFriendsValue.setText("0");
		NumofFriendsValue.setColumns(10);
		
		JLabel label_5 = new JLabel("Value");
		
		AvgstarsValue = new JTextField();
		AvgstarsValue.setText("0");
		AvgstarsValue.setColumns(10);
		
		JLabel lblNewLabel_1 = new JLabel("No.of Votes");
		
		 userVotesCond = new JComboBox();
		userVotesCond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
		JLabel label_6 = new JLabel("Value");
		
		uservotesValue = new JTextField();
		uservotesValue.setText("0");
		uservotesValue.setColumns(10);
		GroupLayout gl_userpanel = new GroupLayout(userpanel);
		gl_userpanel.setHorizontalGroup(
			gl_userpanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_userpanel.createSequentialGroup()
					.addContainerGap()
					.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
						.addGroup(Alignment.TRAILING, gl_userpanel.createSequentialGroup()
							.addComponent(memberSince, GroupLayout.DEFAULT_SIZE, 79, Short.MAX_VALUE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(memberSinceValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addGap(92))
						.addGroup(Alignment.TRAILING, gl_userpanel.createSequentialGroup()
							.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
								.addGroup(gl_userpanel.createSequentialGroup()
									.addComponent(lblReviewCount)
									.addGap(18)
									.addComponent(reviewcountcond, GroupLayout.PREFERRED_SIZE, 63, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(label_3, GroupLayout.PREFERRED_SIZE, 26, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(reviewcountValue, GroupLayout.PREFERRED_SIZE, 45, GroupLayout.PREFERRED_SIZE))
								.addGroup(Alignment.TRAILING, gl_userpanel.createSequentialGroup()
									.addGroup(gl_userpanel.createParallelGroup(Alignment.TRAILING)
										.addGroup(Alignment.LEADING, gl_userpanel.createSequentialGroup()
											.addComponent(lblAve)
											.addGap(23))
										.addGroup(Alignment.LEADING, gl_userpanel.createSequentialGroup()
											.addComponent(lblNoofFriends, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
											.addPreferredGap(ComponentPlacement.RELATED))
										.addGroup(gl_userpanel.createSequentialGroup()
											.addComponent(lblSelect, GroupLayout.PREFERRED_SIZE, 56, GroupLayout.PREFERRED_SIZE)
											.addGap(18)))
									.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
										.addGroup(gl_userpanel.createSequentialGroup()
											.addComponent(friends_cond, GroupLayout.PREFERRED_SIZE, 63, GroupLayout.PREFERRED_SIZE)
											.addPreferredGap(ComponentPlacement.RELATED)
											.addComponent(label_4, GroupLayout.PREFERRED_SIZE, 26, GroupLayout.PREFERRED_SIZE)
											.addPreferredGap(ComponentPlacement.RELATED)
											.addComponent(NumofFriendsValue, GroupLayout.DEFAULT_SIZE, 49, Short.MAX_VALUE))
										.addComponent(maincond, GroupLayout.PREFERRED_SIZE, 119, GroupLayout.PREFERRED_SIZE)
										.addGroup(gl_userpanel.createSequentialGroup()
											.addGap(2)
											.addGroup(gl_userpanel.createParallelGroup(Alignment.TRAILING)
												.addComponent(userVotesCond, GroupLayout.PREFERRED_SIZE, 63, GroupLayout.PREFERRED_SIZE)
												.addComponent(avgstarscond, GroupLayout.PREFERRED_SIZE, 63, GroupLayout.PREFERRED_SIZE))
											.addGap(18)
											.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
												.addGroup(gl_userpanel.createSequentialGroup()
													.addComponent(label_5, GroupLayout.PREFERRED_SIZE, 26, GroupLayout.PREFERRED_SIZE)
													.addPreferredGap(ComponentPlacement.RELATED)
													.addComponent(AvgstarsValue, GroupLayout.PREFERRED_SIZE, 49, GroupLayout.PREFERRED_SIZE))
												.addGroup(gl_userpanel.createSequentialGroup()
													.addComponent(label_6, GroupLayout.PREFERRED_SIZE, 26, GroupLayout.PREFERRED_SIZE)
													.addPreferredGap(ComponentPlacement.RELATED)
													.addComponent(uservotesValue, GroupLayout.PREFERRED_SIZE, 49, GroupLayout.PREFERRED_SIZE)))))
									.addPreferredGap(ComponentPlacement.RELATED)))
							.addGap(30))
						.addGroup(gl_userpanel.createSequentialGroup()
							.addComponent(lblNewLabel_1)
							.addContainerGap(215, Short.MAX_VALUE))))
		);
		gl_userpanel.setVerticalGroup(
			gl_userpanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_userpanel.createSequentialGroup()
					.addGap(4)
					.addGroup(gl_userpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(memberSince)
						.addComponent(memberSinceValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
						.addGroup(gl_userpanel.createParallelGroup(Alignment.BASELINE)
							.addComponent(lblReviewCount)
							.addComponent(reviewcountcond, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(gl_userpanel.createSequentialGroup()
							.addGap(3)
							.addComponent(label_3))
						.addComponent(reviewcountValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
						.addGroup(gl_userpanel.createParallelGroup(Alignment.BASELINE)
							.addComponent(lblNoofFriends)
							.addComponent(friends_cond, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(gl_userpanel.createSequentialGroup()
							.addGap(3)
							.addComponent(label_4))
						.addComponent(NumofFriendsValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(6)
					.addGroup(gl_userpanel.createParallelGroup(Alignment.LEADING)
						.addGroup(gl_userpanel.createSequentialGroup()
							.addGap(3)
							.addGroup(gl_userpanel.createParallelGroup(Alignment.BASELINE)
								.addComponent(label_5)
								.addComponent(avgstarscond, GroupLayout.PREFERRED_SIZE, 17, GroupLayout.PREFERRED_SIZE)))
						.addComponent(lblAve)
						.addComponent(AvgstarsValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_userpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblNewLabel_1)
						.addComponent(userVotesCond, GroupLayout.PREFERRED_SIZE, 17, GroupLayout.PREFERRED_SIZE)
						.addComponent(label_6)
						.addComponent(uservotesValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED, 10, Short.MAX_VALUE)
					.addGroup(gl_userpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(maincond, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(lblSelect))
					.addContainerGap())
		);
		userpanel.setLayout(gl_userpanel);
		
		JLabel label = new JLabel("From");
		
		JLabel label_1 = new JLabel("To");
		
		Fromdate_textField = new JTextField();
		Fromdate_textField.setText("MM/DD/YYYY");
		Fromdate_textField.setColumns(10);
		
		todatetextField = new JTextField();
		todatetextField.setText("MM/DD/YYYY");
		todatetextField.setColumns(10);
		
		JLabel lblNewLabel = new JLabel("Stars");
		
		 reviewstarsCond = new JComboBox();
		reviewstarsCond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
		Stars_value = new JTextField();
		Stars_value.setText("0");
		Stars_value.setColumns(10);
		
		JLabel lblVotes = new JLabel("Votes");
		
		 reviewvotesCond = new JComboBox();
		reviewvotesCond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
		JLabel lblValue_1 = new JLabel("Value");
		
		JLabel label_2 = new JLabel("Value");
		
		votesValue = new JTextField();
		votesValue.setText("0");
		votesValue.setColumns(10);
		GroupLayout gl_reviewpanel = new GroupLayout(reviewpanel);
		gl_reviewpanel.setHorizontalGroup(
			gl_reviewpanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_reviewpanel.createSequentialGroup()
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.LEADING)
						.addGroup(Alignment.TRAILING, gl_reviewpanel.createSequentialGroup()
							.addComponent(lblVotes)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(reviewvotesCond, GroupLayout.PREFERRED_SIZE, 75, GroupLayout.PREFERRED_SIZE))
						.addGroup(Alignment.TRAILING, gl_reviewpanel.createSequentialGroup()
							.addContainerGap()
							.addGroup(gl_reviewpanel.createParallelGroup(Alignment.TRAILING)
								.addComponent(label_2, GroupLayout.PREFERRED_SIZE, 26, GroupLayout.PREFERRED_SIZE)
								.addComponent(lblNewLabel))
							.addPreferredGap(ComponentPlacement.RELATED)
							.addGroup(gl_reviewpanel.createParallelGroup(Alignment.LEADING, false)
								.addComponent(Stars_value, Alignment.TRAILING, 0, 0, Short.MAX_VALUE)
								.addComponent(reviewstarsCond, Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 75, GroupLayout.PREFERRED_SIZE)))
						.addGroup(gl_reviewpanel.createSequentialGroup()
							.addContainerGap()
							.addComponent(lblValue_1)
							.addPreferredGap(ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
							.addComponent(votesValue, GroupLayout.PREFERRED_SIZE, 75, GroupLayout.PREFERRED_SIZE))
						.addGroup(gl_reviewpanel.createSequentialGroup()
							.addContainerGap()
							.addGroup(gl_reviewpanel.createParallelGroup(Alignment.TRAILING)
								.addComponent(label_1, GroupLayout.PREFERRED_SIZE, 21, GroupLayout.PREFERRED_SIZE)
								.addComponent(label, GroupLayout.PREFERRED_SIZE, 24, GroupLayout.PREFERRED_SIZE))
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addGroup(gl_reviewpanel.createParallelGroup(Alignment.LEADING)
								.addComponent(Fromdate_textField, GroupLayout.DEFAULT_SIZE, 84, Short.MAX_VALUE)
								.addComponent(todatetextField, GroupLayout.DEFAULT_SIZE, 84, Short.MAX_VALUE))))
					.addContainerGap())
		);
		gl_reviewpanel.setVerticalGroup(
			gl_reviewpanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_reviewpanel.createSequentialGroup()
					.addContainerGap()
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(label)
						.addComponent(Fromdate_textField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(18)
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(todatetextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(label_1))
					.addGap(18)
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblNewLabel)
						.addComponent(reviewstarsCond, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(18)
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(Stars_value, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(label_2))
					.addGap(18)
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblVotes)
						.addComponent(reviewvotesCond, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_reviewpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblValue_1)
						.addComponent(votesValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(98, Short.MAX_VALUE))
		);
		reviewpanel.setLayout(gl_reviewpanel);
		
		 fromday = new JComboBox();
		fromday.setModel(new DefaultComboBoxModel(new String[] {"Day", "0-Sunday", "1-Monday", "2-Tuesday", "3-Wednesday", "4-Thursday", "5-Friday", "6-Saturday"}));
		
		JLabel lblFrom = new JLabel("From");
		
		 fromhour = new JComboBox();
		fromhour.setModel(new DefaultComboBoxModel(new String[] {"hour", "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"}));
		
		 checkin_todate = new JComboBox();
		checkin_todate.setModel(new DefaultComboBoxModel(new String[] {"Day", "0-Sunday", "1-Monday", "2-Tuesday", "3-Wednesday", "4-Thursday", "5-Friday", "6-Saturday"}));
		
		checkintohour = new JComboBox();
		checkintohour.setModel(new DefaultComboBoxModel(new String[] {"hour", "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"}));
		
		JLabel lblTo = new JLabel("To");
		
		JLabel lblNoOfCheckins = new JLabel("No. of Checkins");
		
		 noofcheckincond = new JComboBox();
		noofcheckincond.setModel(new DefaultComboBoxModel(new String[] {"=, >, <", "=", ">", "<"}));
		
		JLabel lblValue = new JLabel("Value:");
		
		numberofcheckValues = new JTextField();
		numberofcheckValues.setText("0");
		numberofcheckValues.setColumns(10);
		GroupLayout gl_checkinpanel = new GroupLayout(checkinpanel);
		gl_checkinpanel.setHorizontalGroup(
			gl_checkinpanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_checkinpanel.createSequentialGroup()
					.addGroup(gl_checkinpanel.createParallelGroup(Alignment.LEADING)
						.addComponent(lblFrom)
						.addComponent(lblTo, GroupLayout.PREFERRED_SIZE, 21, GroupLayout.PREFERRED_SIZE)
						.addComponent(lblNoOfCheckins)
						.addGroup(gl_checkinpanel.createSequentialGroup()
							.addComponent(lblValue)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(numberofcheckValues, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(gl_checkinpanel.createSequentialGroup()
							.addContainerGap()
							.addGroup(gl_checkinpanel.createParallelGroup(Alignment.LEADING)
								.addComponent(checkin_todate, GroupLayout.PREFERRED_SIZE, 46, GroupLayout.PREFERRED_SIZE)
								.addComponent(fromday, GroupLayout.PREFERRED_SIZE, 46, GroupLayout.PREFERRED_SIZE))
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addGroup(gl_checkinpanel.createParallelGroup(Alignment.LEADING)
								.addComponent(fromhour, 0, 54, Short.MAX_VALUE)
								.addComponent(checkintohour, 0, 54, Short.MAX_VALUE))))
					.addContainerGap())
				.addGroup(gl_checkinpanel.createSequentialGroup()
					.addGap(21)
					.addComponent(noofcheckincond, 0, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
					.addGap(34))
		);
		gl_checkinpanel.setVerticalGroup(
			gl_checkinpanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_checkinpanel.createSequentialGroup()
					.addGap(4)
					.addComponent(lblFrom)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(gl_checkinpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(fromday, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(fromhour, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(lblTo)
					.addGap(3)
					.addGroup(gl_checkinpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(checkin_todate, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(checkintohour, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(lblNoOfCheckins)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(noofcheckincond, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(11)
					.addGroup(gl_checkinpanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblValue)
						.addComponent(numberofcheckValues, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(123, Short.MAX_VALUE))
		);
		checkinpanel.setLayout(gl_checkinpanel);
		
		resulttable = new JTable();
		resulttable.setModel(new DefaultTableModel(
			new Object[][] {
			},
			new String[] {
				"New column", "New column", "New column", "New column"
			}
		));
		resultPane.setViewportView(resulttable);
		
		JScrollPane scrollPane = new JScrollPane();
		
		JScrollPane scrollPane_1 = new JScrollPane();
		
		JButton btnPopulateSubcat = new JButton("Populate Subcat");
		btnPopulateSubcat.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				
				HW3 asd=new HW3();
				ArrayList subcatogeries = asd.getSubcatogeries(catPanelCheckBoxActionHandler.getCategoriesSelected());
				//PopulatePannel2(subcatogeries);
			}
		});
		
		JScrollPane scrollPane_2 = new JScrollPane();
		
		 SubCategoryPanel = new JPanel();
		 SubCategoryPanel.setBorder(new TitledBorder(UIManager.getBorder("TitledBorder.border"), "", TitledBorder.LEADING, TitledBorder.TOP, null, null));
		scrollPane_1.setViewportView(SubCategoryPanel);
		
		 pointOfInterestComboBox = new JComboBox();
		
		 proximityComboBox = new JComboBox();
		proximityComboBox.setModel(new DefaultComboBoxModel(new String[] {"None", "5", "10", "20", "30", "40", "50"}));
		
		 searchForComboBox = new JComboBox();
		searchForComboBox.setModel(new DefaultComboBoxModel(new String[] {" and , or", "and", "or"}));
		
		JLabel lblPointOfIntrest = new JLabel("Point of intrest");
		
		JLabel lblProximity = new JLabel("Proximity");
		
		JLabel lblConditions = new JLabel("Conditions");
		
		JLabel lblMiles = new JLabel("Miles");
		GroupLayout gl_SubCategoryPanel = new GroupLayout(SubCategoryPanel);
		gl_SubCategoryPanel.setHorizontalGroup(
			gl_SubCategoryPanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_SubCategoryPanel.createSequentialGroup()
					.addGap(10)
					.addGroup(gl_SubCategoryPanel.createParallelGroup(Alignment.LEADING, false)
						.addComponent(lblPointOfIntrest)
						.addComponent(pointOfInterestComboBox, GroupLayout.PREFERRED_SIZE, 96, GroupLayout.PREFERRED_SIZE)
						.addComponent(lblProximity)
						.addComponent(searchForComboBox, GroupLayout.PREFERRED_SIZE, 96, GroupLayout.PREFERRED_SIZE)
						.addGroup(gl_SubCategoryPanel.createSequentialGroup()
							.addGroup(gl_SubCategoryPanel.createParallelGroup(Alignment.TRAILING, false)
								.addComponent(proximityComboBox, Alignment.LEADING, 0, 0, Short.MAX_VALUE)
								.addComponent(lblConditions, Alignment.LEADING, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
							.addGap(18)
							.addComponent(lblMiles, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
					.addContainerGap(10, Short.MAX_VALUE))
		);
		gl_SubCategoryPanel.setVerticalGroup(
			gl_SubCategoryPanel.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_SubCategoryPanel.createSequentialGroup()
					.addGap(5)
					.addComponent(lblPointOfIntrest)
					.addGap(6)
					.addComponent(pointOfInterestComboBox, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(lblProximity)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_SubCategoryPanel.createParallelGroup(Alignment.BASELINE)
						.addComponent(proximityComboBox, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(lblMiles))
					.addGap(29)
					.addComponent(lblConditions)
					.addGap(6)
					.addComponent(searchForComboBox, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
		);
		SubCategoryPanel.setLayout(gl_SubCategoryPanel);
		
		 MainCategoryPanel = new JPanel();
		scrollPane.setViewportView(MainCategoryPanel);
		
		 rdbtnBusinessSearch = new JRadioButton("Business Search");
		 rdbtnBusinessSearch.addActionListener(new ActionListener() {
		 	public void actionPerformed(ActionEvent e) {
		 		//setbusinessto false
		 		if(rdbtnBusinessSearch.isSelected())
		 			rdbtnUserSearch.setSelected(false);
		 	System.out.println("rdbtnUserSearch"+rdbtnUserSearch.isSelected());
		 	}
		 });
	
		 rdbtnUserSearch = new JRadioButton("User Search");
		 rdbtnUserSearch.addActionListener(new ActionListener() {
		 	public void actionPerformed(ActionEvent e) {
		 		//setbusinessto false
		 		if(rdbtnUserSearch.isSelected())
		 			rdbtnBusinessSearch.setSelected(false);
		 	System.out.println("rdbtnBusinessSearch "+rdbtnBusinessSearch.isSelected());
		 	}
		 });
		GroupLayout groupLayout = new GroupLayout(frame.getContentPane());
		groupLayout.setHorizontalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(groupLayout.createSequentialGroup()
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
							.addGap(10)
							.addComponent(scrollPane, GroupLayout.PREFERRED_SIZE, 105, GroupLayout.PREFERRED_SIZE))
						.addComponent(btnPopulateSubcat))
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
							.addGap(6)
							.addComponent(scrollPane_1, GroupLayout.PREFERRED_SIZE, 122, GroupLayout.PREFERRED_SIZE))
						.addGroup(groupLayout.createSequentialGroup()
							.addGap(30)
							.addComponent(rdbtnBusinessSearch)))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addComponent(rdbtnUserSearch)
						.addComponent(checkinpanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(18)
					.addComponent(reviewpanel, GroupLayout.PREFERRED_SIZE, 136, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(resultPane, GroupLayout.PREFERRED_SIZE, 230, GroupLayout.PREFERRED_SIZE))
				.addGroup(groupLayout.createSequentialGroup()
					.addGap(10)
					.addComponent(userpanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addComponent(scrollPane_2, GroupLayout.PREFERRED_SIZE, 376, GroupLayout.PREFERRED_SIZE)
						.addGroup(groupLayout.createSequentialGroup()
							.addGap(50)
							.addComponent(btnNewButton))))
		);
		groupLayout.setVerticalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(groupLayout.createSequentialGroup()
					.addGap(11)
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
							.addComponent(scrollPane, GroupLayout.PREFERRED_SIZE, 260, GroupLayout.PREFERRED_SIZE)
							.addGap(6)
							.addComponent(btnPopulateSubcat))
						.addComponent(reviewpanel, GroupLayout.PREFERRED_SIZE, 256, GroupLayout.PREFERRED_SIZE)
						.addComponent(resultPane, GroupLayout.PREFERRED_SIZE, 281, GroupLayout.PREFERRED_SIZE)
						.addGroup(groupLayout.createParallelGroup(Alignment.TRAILING, false)
							.addComponent(checkinpanel, Alignment.LEADING, 0, 0, Short.MAX_VALUE)
							.addComponent(scrollPane_1, Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 260, Short.MAX_VALUE)))
					.addGap(6)
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addComponent(userpanel, GroupLayout.PREFERRED_SIZE, 191, GroupLayout.PREFERRED_SIZE)
						.addGroup(groupLayout.createSequentialGroup()
							.addGap(12)
							.addComponent(scrollPane_2, GroupLayout.PREFERRED_SIZE, 136, GroupLayout.PREFERRED_SIZE)
							.addGap(20)
							.addComponent(btnNewButton))))
				.addGroup(Alignment.TRAILING, groupLayout.createSequentialGroup()
					.addContainerGap(281, Short.MAX_VALUE)
					.addGroup(groupLayout.createParallelGroup(Alignment.BASELINE)
						.addComponent(rdbtnBusinessSearch)
						.addComponent(rdbtnUserSearch))
					.addGap(226))
		);
			AddressClass address1 = new AddressClass("4237 Lien Rd\nSte H\nMayfair Park\nMadison, WI 53704", -89.3083801269531, 43.1205749511719);
			AddressClass address2 = new AddressClass("4840 E Indian School Rd\nSte 101\nPhoenix, AZ 85018",  -111.983757019043, 33.4993133544922);
			AddressClass address3 = new AddressClass("Mesa, AZ 85206", -111.701843261719, 33.3951606750488);
			AddressClass address4 = new AddressClass("3921 E Baseline Rd\nSte 108\nGilbert, AZ 85234",  -111.747520446777, 33.3782119750977);
			AddressClass address5 = new AddressClass("1610 Lake Las Vegas Pkwy\nHenderson, NV 89011", -114.932034, 36.102125000000001);
			
			Addressmap.put(address2.Address, address2);
			Addressmap.put(address1.Address, address1);
			Addressmap.put(address3.Address, address3);
			Addressmap.put(address4.Address, address4);
			Addressmap.put(address5.Address, address5);
			pointOfInterestComboBox.setModel(new DefaultComboBoxModel(new String[] {"NONE",address2.Address,address3.Address ,address4.Address ,address5.Address}));
	        
		 queryPane = new JTextPane();
		scrollPane_2.setViewportView(queryPane);
		frame.getContentPane().setLayout(groupLayout);
		populateCategoriesPanel() ;
		try {
			connection();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	public static DefaultTableModel buildTableModel(ResultSet rs) throws SQLException{
		  ResultSetMetaData rsmd = rs.getMetaData();
		 
		  //column names
		  Vector<String> columnNames = new Vector<String>();
		  int columnCount = rsmd.getColumnCount();
		  for (int column = 1; column <= columnCount; column++){
		      columnNames.add(rsmd.getColumnName(column));
		  }
		 
		  //data of the table
		  Vector<Vector<Object>>data = new Vector<Vector<Object>>();
		  while(rs.next()){
		      Vector<Object> vector = new Vector<Object>();
		      for(int columnIndex = 1; columnIndex<=columnCount; columnIndex++){
		    	  Object object = rs.getObject(columnIndex);
		    	 
		    	  
		    	  if(object instanceof Clob){
		    		  
		    		  
		    		  
		    		  Clob clob = (Clob)object;

		    		    if (clob != null) {

		    		      if ((int) clob.length() > 0) {
		    		        String s = clob.getSubString(1, (int) clob.length());
		    		        // Do something with string.
		    		        System.out.println("String "+s);
		    		        vector.add(s);
		    		      }
		    		    }
		    	  }else{
		    		  vector.add(object);
		    	  }
/*		    	  if (if (clob != null) {

		    	      if ((int) clob.length() > 0) {
		    	        String s = clob.getSubString(1, (int) clob.length());
		    	        // Do something with string.
		    	      }
		    	    }*/
		      }
		      data.add(vector);
		  }
		  return new DefaultTableModel(data,columnNames);
		}
	
private void Table_4_user(String query) throws SQLException{
	Connection dbConnection = YelpDBConnectionFactory.connectDatabase();
	Statement st = dbConnection.createStatement();
	ResultSet rs = st.executeQuery(query);
	if(rs != null){
		resulttable = new JTable(buildTableModel(rs));
		resulttable.removeColumn(resulttable.getColumnModel().getColumn(0));
		resultPane.add(resulttable);
		resultPane.setViewportView(resulttable);
		resultPane.revalidate();
		resultPane.repaint();
		resulttable.addMouseListener(new MouseListener() {
			  	
			@Override
			public void mouseReleased(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mousePressed(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseExited(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseEntered(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseClicked(MouseEvent e) {
				int row = resulttable.rowAtPoint(e.getPoint());
				String y_id = resulttable.getModel().getValueAt(row, 0).toString();
				if (row>=0)
				{
					JFrame review = new JFrame();
					review.setSize(600, 700);
					
					review.setVisible(true);
					System.out.println(y_id);
					String ReviewQuery = "select R_PUBLISH_DATE, R_STARS, R_REVIEW_TEXT, R_AUTHOR from Y_REVIEWS_TB where  R_AUTHOR = '"+y_id+"'";
					try {
						Connection dbConnection = YelpDBConnectionFactory.connectDatabase();
						Statement st = dbConnection.createStatement();
						ResultSet reviewResults = st.executeQuery(ReviewQuery);
						table_review = new JTable(buildTableModel(reviewResults));
					} catch (SQLException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					Review_pane = new JScrollPane();
					review.getContentPane().add(Review_pane);
					Review_pane.add(table_review);
					Review_pane.setViewportView(table_review);
					Review_pane.revalidate();
					Review_pane.repaint();
					review.revalidate();
					review.repaint();			
				}
		}
	});
	}
	else{
		System.out.println("No Result");
		JOptionPane.showMessageDialog(null,
			    "No Result",
			    "Search Warning",
			    JOptionPane.WARNING_MESSAGE);
	}
}
private void Table_4_business(String query) throws SQLException{
	Connection dbConnection = YelpDBConnectionFactory.connectDatabase();
	Statement st = dbConnection.createStatement();
	ResultSet rs = st.executeQuery(query);
	if(rs != null){
		resulttable = new JTable(buildTableModel(rs));
		resulttable.removeColumn(resulttable.getColumnModel().getColumn(0));
		resultPane.add(resulttable);
		resultPane.setViewportView(resulttable);
		resultPane.revalidate();
		resultPane.repaint();
		resulttable.addMouseListener(new MouseListener() {
			  	
			@Override
			public void mouseReleased(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mousePressed(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseExited(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseEntered(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseClicked(MouseEvent e) {
				int row = resulttable.rowAtPoint(e.getPoint());
				String b_id = resulttable.getModel().getValueAt(row, 0).toString();
				if (row>=0)
				{
					JFrame review = new JFrame();
					review.setSize(500, 500);
					
					review.setVisible(true);
					System.out.println(b_id);
					String ReviewQuery = "select R_PUBLISH_DATE, R_STARS, R_REVIEW_TEXT, R_AUTHOR from Y_REVIEWS_TB where  R_BUSS_ID = '"+b_id+"'";
					try {
						Connection dbConnection = YelpDBConnectionFactory.connectDatabase();
						Statement st = dbConnection.createStatement();
						ResultSet reviewResults = st.executeQuery(ReviewQuery);
						table_review = new JTable(buildTableModel(reviewResults));
					} catch (SQLException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					Review_pane = new JScrollPane();
					review.getContentPane().add(Review_pane);
					Review_pane.add(table_review);
					Review_pane.setViewportView(table_review);
					Review_pane.revalidate();
					Review_pane.repaint();
					review.revalidate();
					review.repaint();			
				}
		}
	});
	}
	else{
		System.out.println("No Result");
		JOptionPane.showMessageDialog(null,
			    "No Result",
			    "Search Warning",
			    JOptionPane.WARNING_MESSAGE);
	}
}
private void connection() throws Exception{
    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    if (client != null)
    {
    	
    }
}

	public void populateCategoriesPanel() {

		// Call database to get the master categories
		Connection dbConnection = YelpDBConnectionFactory.connectDatabase();
		JCheckBox chckbxNewCheckBox;
		String query = "select categorypk from business28category";
		ResultSet resultSet;
		Statement statement = null;
		int totalRows = 0;
		try {
		
			
			System.out.println("Total Rows : " + totalRows);
			//resultSet = statement.executeQuery(query);
			this.catPanelCheckBoxActionHandler = new CatPanelCheckBoxActionHandler();
			String[] allCategories = {"Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists", "Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical", "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening", "Nightlife", "Restaurants", "Shopping", "Transportation"};
		
			// Populate the panel with check boxes
			for (String s: allCategories){
				String catName = s;
				System.out.println("Cat Name : " + catName);
				chckbxNewCheckBox = new JCheckBox(catName);
				
				chckbxNewCheckBox.addActionListener(catPanelCheckBoxActionHandler);
				MainCategoryPanel.add(chckbxNewCheckBox);
			}
			
		} catch (Exception exec) {
			System.out.println(exec);
		}
		MainCategoryPanel.setLayout(new GridLayout(totalRows, 1));
	}
	public void PopulatePannel2(ArrayList subcatogeries){
		// Call database to get the master categories
		Connection dbConnection = YelpDBConnectionFactory.connectDatabase();
		JCheckBox chckbxNewCheckBox;
		SubCategoryPanel.removeAll();
		ResultSet resultSet;
		Statement statement = null;
		int totalRows = 0;
		int trows=0;
		try {
			/*statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			resultSet = statement.executeQuery(queryPart);
			resultSet.last();
			totalRows = resultSet.getRow();
			resultSet.beforeFirst();
			
			System.out.println("Total Rows : " + totalRows);
			
			resultSet = statement.executeQuery(queryPart);*/
			this.SubcatPanelCheckBoxActionHandler = new CatPanelCheckBoxActionHandler();
			Iterator iterator = subcatogeries.iterator();
			// Populate the panel with check boxes
			while (iterator.hasNext()) {
				String catName = (String)iterator.next();
				System.out.println("Cat Name : " + catName);
				chckbxNewCheckBox = new JCheckBox(catName);
				
				chckbxNewCheckBox.addActionListener(SubcatPanelCheckBoxActionHandler);
				SubCategoryPanel.add(chckbxNewCheckBox);
			}
			
		} catch (Exception exec) {
			System.out.println(exec);
		}
		
		trows=trows+totalRows;
		SubCategoryPanel.setLayout(new GridLayout(trows, 1));
		SubCategoryPanel.revalidate();
		SubCategoryPanel.repaint();
	}
	
	public void populateuserreview(String username){
		System.out.println("businessName = "+username);
    	JFrame reviewframe = new JFrame();
		reviewframe.setSize(600, 600);
		reviewframe.setTitle("Reviews Table");
		reviewframe.setVisible(true);
		reviewTable = new JTable();
		String[] columnNames = new String[]{"User Name","Review Date", "Stars", "Review Text"};
		DefaultTableModel model1 = new DefaultTableModel(columnNames, 0);
		reviewTable.setModel(model1);
		reviewTable.setShowGrid(true);
		
    	
		MongoClient client;
		String searchForComboBox ="and";
		
	    client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
	    DB db1 = new MongoClient("localhost",27017).getDB("YelpApplication");
      	DBCollection dbcollection=db1.getCollection("YelpUser");
      	
      	BasicDBObject proj1 = new BasicDBObject();
		
		String reviewcond = "$gte";
		  String option ="$and";
		BasicDBObject votescountcond1 = new BasicDBObject("name",username);
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
			break;
		} catch (Exception e) {
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
	         System.out.println("match asda ="+match);
	      	  project = new BasicDBObject("$project", new BasicDBObject("text", 1).append("stars", 1).append("totalVotes", 1).append("date", 1).append("_id", 0));
	      	
	         System.out.println(project );
	         AggregationOutput output1 = dbcollection.aggregate(match,project);
	      	System.out.println("output"+output);
	       // query = match.toString();
	      	ArrayList businessiidList= new ArrayList<String>();
	      	 
    	try {
			for(DBObject str: output1.results()){
				System.out.println("list is: "+str.toString().trim());
   			   	JSONParser parser = new JSONParser();
   			   	JSONObject jsonObject = (JSONObject) parser.parse(str.toString().trim());
   			   	JSONObject dated = (JSONObject) jsonObject.get("date");
   			   	String date = (String)dated.get("$date");
   			   	
   			 Long stars = (Long) jsonObject.get("stars");
   			   	String text = (String) jsonObject.get("text");
   			   		Double tvotes= (Double) jsonObject.get("totalVotes");
   			   	String user_name = username;
   			   	
   			   	model1.addRow(new Object[]{user_name,date, stars.toString(), text});
   			   	
   			   	System.out.println(date+"date");
			}
		}
   		catch(Exception e){
   			System.out.println("Cant execute " + e);
   			e.printStackTrace();
   			}
    	
    	scrollPane_review = new JScrollPane();
		reviewframe.getContentPane().add(scrollPane_review);
		scrollPane_review.add(reviewTable);
		scrollPane_review.setViewportView(reviewTable);
		scrollPane_review.revalidate();
		scrollPane_review.repaint();
		System.out.println("Done" );
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
	
	public void populateReview(String businessName){
		System.out.println("businessName = "+businessName);
    	JFrame reviewframe = new JFrame();
		reviewframe.setSize(600, 600);
		reviewframe.setTitle("Reviews Table");
		reviewframe.setVisible(true);
		reviewTable = new JTable();
		String[] columnNames = new String[]{ "User Name","Stars", "Review Text","Votes"};
		DefaultTableModel model1 = new DefaultTableModel(columnNames, 0);
		reviewTable.setModel(model1);
		reviewTable.setShowGrid(true);
		
    	MongoCollection<Document> b_collection=client.getDatabase("YelpApplication").getCollection("YelpBusiness");
    	
    	
    	
    	AggregateIterable<Document> list = b_collection.aggregate(asList(match(eq("name", businessName))));
    			b_collection=client.getDatabase("YelpApplication").getCollection("YelpBusiness");
    			list = b_collection.aggregate(asList(match(eq("name", businessName)), 
    																lookup("YelpReview", "business_id", "business_id", "YelpBusinessReviews"), 
    																unwind("$YelpBusinessReviews"),
    																lookup("YelpUser", "YelpBusinessReviews.user_id", "user_id", "UserDetails"),
    																project(fields(computed("date", "$YelpBusinessReviews.date"), computed("stars", "$YelpBusinessReviews.stars"), computed("text", "$YelpBusinessReviews.text"), computed("userName", "$UserDetails.name"), computed("usefulVotes", "$YelpBusinessReviews.votes.useful"),  excludeId())),
    																unwind("$userName"),limit(50)));
    	
    	
    	
    	
    			int count = 0;
    	System.out.println("b_collection"+b_collection);
    	try {
			for(Document str:list){
				System.out.println("list is: "+str.toJson().toString().trim());
   			   	JSONParser parser = new JSONParser();
   			   	JSONObject jsonObject = (JSONObject) parser.parse(str.toJson().toString().trim());
   			 JSONObject js1 = (JSONObject)jsonObject.get("date");
   			 			
   			   	//Date date = (Date) js1.get("$date");
   			Long stars = (Long) jsonObject.get("stars");
   			   	String text = (String) jsonObject.get("text");
   			   	String user_name = (String) jsonObject.get("userName");
   			   	String votesUseful=(String) jsonObject.get("usefulVotes").toString();
   			   	model1.addRow(new Object[]{ user_name,stars.toString(), text,votesUseful});
   			 count++;
   			   	
			}
			System.out.println("model1 updated"+model1);
		}
   		catch(Exception e){
   			System.out.println("Cant execute " + e);
   			e.printStackTrace();
   			}
    	
    	scrollPane_review = new JScrollPane();
		reviewframe.getContentPane().add(scrollPane_review);
		scrollPane_review.add(reviewTable);
		scrollPane_review.setViewportView(reviewTable);
		scrollPane_review.revalidate();
		scrollPane_review.repaint();
		System.out.println("Done" );
}
	public class AddressClass{
		public String Address;
		public double Lonlongitude;
		public double Latitude;
		public AddressClass(String Address, double d, double e){
			this.Address = Address;
			this.Lonlongitude = d;
			this.Latitude = e;
		}
	}

class CatPanelCheckBoxActionHandler implements ActionListener {
	
	 ArrayList<String> categoriesSelected = null;
	
	
	
	public CatPanelCheckBoxActionHandler() {
		this.categoriesSelected = new ArrayList<String>();
	}

	@Override
	public void actionPerformed(ActionEvent event) {
		// TODO Auto-generated method stub
		JCheckBox cb = (JCheckBox) event.getSource();
        if (cb.isSelected()) {
            System.out.println("Checkbox "+cb.getText()+" is checked");
            //this.getCategoriesSelected().add(cb.getText());
            categoriesSelected.add(cb.getText());
            System.out.println(categoriesSelected.size());
        } else {
        	System.out.println("Checkbox "+cb.getText()+" is unchecked");
        	this.getCategoriesSelected().remove(cb.getText());
        }
        
        
        for (Iterator iterator = categoriesSelected.iterator(); iterator
				.hasNext();) {
			String maincategories = (String) iterator.next();
			
		}
        

	}

	/**
	 * @return the categoriesSelected
	 */
	public ArrayList<String> getCategoriesSelected() {
		return this.categoriesSelected;
	}

	/**
	 * @param categoriesSelected the categoriesSelected to set
	 */
	public void setCategoriesSelected(ArrayList<String> categoriesSelected) {
		this.categoriesSelected = categoriesSelected;
	}
	
	public void pane2Query(){
		System.out.println(categoriesSelected);
	}
	


}
}



