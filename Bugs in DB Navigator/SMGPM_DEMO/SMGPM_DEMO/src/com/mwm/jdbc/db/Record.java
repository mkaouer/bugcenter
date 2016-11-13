package com.mwm.jdbc.db;
//java SQL
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
//java utilities
import java.util.ArrayList;
import java.util.Date;

public class Record 
{
	Integer ID;
	Date ActivityDate;
	String CampaignName ;
	String DFA_Site_Name;
	String Placement_Group_Strategy;
	String SitePlacementName ;
	String SiteContentCategory;
	String ContentCatGroup;
	String Product;
	String ProductType;
	Double Profitability;
	Double MediaCost;
	Double EstimatedBalance_1;
	Double DFA_Accounts ;
	Double ActivityRevenue_1 ; // PathFinder Deposits
	Double DFASumImps ;
	Double DFASumClicks ;
	Double Attribution;
	Double DFA_Revenue;
	String MobileDevice ;
	String DeviceViewType ;
	String DeviceOS ;
	String TotalProduct;
	String Site_Name;
	
	
	public Record()
	{
		ID = new Integer(0) ;
		ActivityDate = new Date();
		CampaignName = new String();
		DFA_Site_Name = new String();
		Placement_Group_Strategy = new String();
		SiteContentCategory = new String();
		ContentCatGroup = new String();
		Product = new String();
		ProductType = new String();
		Profitability = 0.0;
		MediaCost = 0.0;
		EstimatedBalance_1 = 0.0;
		DFA_Accounts = 0.0;
		ActivityRevenue_1 = 0.0;
		DFASumImps = 0.0;
		DFASumClicks = 0.0;
		Attribution = 0.0;
		DFA_Revenue = 0.0 ;
		MobileDevice = new String();
		DeviceViewType = new String();
		DeviceOS = new String();
		TotalProduct = new String();
		Site_Name = new String();
	}
	
	public int getColumnCount() 
	{
	    return getClass().getDeclaredFields().length;
	}
	
	public void print_record()
	{
		System.out.println(ID+" "+ActivityDate+" "+DFA_Site_Name+" "+Placement_Group_Strategy+" "+SiteContentCategory+" "+ContentCatGroup+" "+Product+" "+ProductType+" "+Profitability+" "+MediaCost);
	}
	
}
