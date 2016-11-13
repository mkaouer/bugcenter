package com.mwm.jdbc.db;

import java.util.ArrayList;

public class ContentCategoryGroup 
{
	public String name;
	public int percentage;
	
	public ArrayList<String> CC;
	public ArrayList<Integer> percentages;
	
	public ContentCategoryGroup(String name, int percentage)
	{
		this.name = new String (name);
		this.percentage = percentage;
		this.CC = new ArrayList<String>();
	}
	
	public ContentCategoryGroup(String name)
	{
		this.name = new String (name);
		this.CC = new ArrayList<String>();
	}
	
	public ContentCategoryGroup(ContentCategoryGroup object)
	{
		this.name = new String(object.name);
		this.percentage = object.percentage;
		this.CC = new ArrayList<String>(object.CC);
	}
	
	public void set_cc_percentage(String cc, int percentage)
	{
		for (int i=0; i<CC.size();i++)
		{
			if (CC.get(i).equals(cc)) percentages.add(percentage);
		}
	}
	
	public void set_ccg_percentage(String ccg, int percentage)
	{
		if (name.equals(ccg)) this.percentage = percentage;
	}
	
	
	
	public void verify_percentages ()
	{
		int sum = 0;
		
		for (int i=0; i<percentages.size();i++)
		{
			sum+= percentages.get(i);
		}
		if (sum != 100)
		{
			System.out.println(" ERROR: percentages dont match 100%");
		}
	}
}
