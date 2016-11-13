package com.mwm.jdbc.db;

import java.util.ArrayList;

public class Strategy 
{
	public String name;
	public int percentage;
	public ArrayList<ContentCategoryGroup> ccg;
	
	public Strategy (String strat_name)
	{
		name = new String(strat_name);
		this.ccg = new ArrayList<ContentCategoryGroup>();
		
		if (name.equals("Awareness"))
		{
			ContentCategoryGroup temp = new ContentCategoryGroup("Display");
			ArrayList<String> str_temp = new ArrayList<String>();
			str_temp.add("Co-branded");
			str_temp.add("Email");
			//str_temp.add("Finance");
			//str_temp.add("Frontpage");
			str_temp.add("Network Push");
			str_temp.add("Co-branded");
			str_temp.add("Portal Homepage");
			str_temp.add("Retirement");
			
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
			//temp = new CCG("Display Mobile");
			//str_temp = new ArrayList<String>();
			//str_temp.add("Enhanced Targeting");
			//str_temp.add("Finance");
			//str_temp.add("Frontpage");
			//str_temp.add("Mobile");
						
			//temp.CC = new ArrayList<String>(str_temp);
			//this.ccg.add(temp);
			
			//temp = new CCG("Display Mobile Video");
			//str_temp = new ArrayList<String>();
			//str_temp.add("Enhanced Targeting");
			//str_temp.add("Finance");
			//str_temp.add("News");
			//str_temp.add("Digital Video");
			//str_temp.add("Display Video");
			//str_temp.add("Behavioral Targeting");
						
			//temp.CC = new ArrayList<String>(str_temp);
			//this.ccg.add(temp);
			
			//temp = new CCG("Display Video");
			//str_temp = new ArrayList<String>();
			//str_temp.add("Frontpage");
			//str_temp.add("Sports");
						
			//temp.CC = new ArrayList<String>(str_temp);
			//this.ccg.add(temp);
			
			temp = new ContentCategoryGroup("Organic Search");
			str_temp = new ArrayList<String>();
			str_temp.add("Organic Search");
						
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
			//temp = new CCG("Paid Search Mobile");
			//str_temp = new ArrayList<String>();
			//str_temp.add("Mobile");
						
			//temp.CC = new ArrayList<String>(str_temp);
			//this.ccg.add(temp);
		}
		
		if (name.equals("Engagement"))
		{
			ContentCategoryGroup temp = new ContentCategoryGroup("Display");
			ArrayList<String> str_temp = new ArrayList<String>();
			str_temp.add("Digital Radio");
			str_temp.add("Educational Finance");
			//str_temp.add("Finance");
			//str_temp.add("Frontpage");
			//str_temp.add("Lifestyle");
			//str_temp.add("News");
			//str_temp.add("Sports");
			
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
			//temp = new CCG("Display Mobile");
			//str_temp = new ArrayList<String>();
			//str_temp.add("Ad Network");
			//str_temp.add("Behavioral Targeting");
			//str_temp.add("Enhanced Targeting");
			//str_temp.add("Finance");
			//str_temp.add("Frontpage");
			//str_temp.add("Lifestyle");
			//str_temp.add("News");
						
			//temp.CC = new ArrayList<String>(str_temp);
			//this.ccg.add(temp);
			
			//temp = new CCG("Display Video");
			//str_temp = new ArrayList<String>();
			//str_temp.add("Enhanced Targeting");
			//str_temp.add("Finance");
			//str_temp.add("News");
			//str_temp.add("Lifestyle");
						
			//temp.CC = new ArrayList<String>(str_temp);
			//this.ccg.add(temp);
		}
		
		if (name.equals("Conversion"))
		{
			ContentCategoryGroup temp = new ContentCategoryGroup("Display");
			ArrayList<String> str_temp = new ArrayList<String>();
			str_temp.add("Acquisition");
			//str_temp.add("Ad Network");
			str_temp.add("BankrateFin");
			str_temp.add("BankrateSpecial");
			//str_temp.add("Behavioral Targeting");
			//str_temp.add("Enhanced Targeting");
			//str_temp.add("Finance");
			str_temp.add("FinanceNoImp");
			//str_temp.add("Frontpage");
			
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
			temp = new ContentCategoryGroup("Affiliate Networks");
			str_temp = new ArrayList<String>();
			str_temp.add("Affiliate Marketing");
						
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
			temp = new ContentCategoryGroup("Paid Search");
			str_temp = new ArrayList<String>();
			str_temp.add("Ipad");
			str_temp.add("IPad");
			str_temp.add("Paid Search");
						
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
			temp = new ContentCategoryGroup("Display Rate Tables");
			str_temp = new ArrayList<String>();
			str_temp.add("Rate Tables");
						
			temp.CC = new ArrayList<String>(str_temp);
			this.ccg.add(temp);
			
		}
		
	}

}
