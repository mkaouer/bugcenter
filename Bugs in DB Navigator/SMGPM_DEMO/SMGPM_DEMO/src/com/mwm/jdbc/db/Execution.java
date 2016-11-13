/*
 * 
 */
package com.mwm.jdbc.db;

import java.io.BufferedReader;
//import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.LocalDate;

import com.mwm.jdbc.db.UserPreferences;
import com.mwm.jdbc.db.ApplicationException;
 
public class Execution 
{
	
	static void update_Profitability(Records p)
	{
    	DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime int0 =    new DateTime(2013, 6, 30, 0, 0);
    	DateTime int1 =    new DateTime(2013, 7, 30, 0, 0);
    	DateTime int2 =    new DateTime(2013, 10, 30, 0, 0);
    	DateTime int3 =    new DateTime(2014, 1, 30, 0, 0);
    	DateTime int4 =    new DateTime(2014, 3, 30, 0, 0);
    	DateTime int5 =    new DateTime(2014, 4, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 5, 30, 12, 0, 0);
    	
    	p.set_array_values_on_period("Profitability", 10.00, 20.00, -2.0, +2.0, start, int0);
    	p.set_array_values_on_period("Profitability", 20.00, 25.00, -2.0, +2.0, int0, int1);
    	p.set_array_values_on_period("Profitability", 25.00, 20.00, -2.0, +2.0, int1, int2);
    	p.set_array_values_on_period("Profitability", 20.00, 25.00, -2.0, +2.0, int2, int3);
    	p.set_array_values_on_period("Profitability", 25.00, 30.00, -2.0, +2.0, int3, int4);
    	p.set_array_values_on_period("Profitability", 32.00, 30.00, -2.0, +2.0, int4, int5);
    	p.set_array_values_on_period("Profitability", 32.00, 35.00, -2.0, +2.0, int5, end);
	}
	
	static void update_ROAS(Records p)
	{
		
    	DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime int0 =    new DateTime(2013, 6, 30, 0, 0);
    	DateTime int1 =    new DateTime(2013, 7, 30, 0, 0);
    	DateTime int2 =    new DateTime(2013, 10, 30, 0, 0);
    	DateTime int3 =    new DateTime(2014, 1, 30, 0, 0);
    	DateTime int4 =    new DateTime(2014, 3, 30, 0, 0);
    	DateTime int5 =    new DateTime(2014, 4, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 5, 30, 12, 0, 0);
    	
    	p.set_array_values_on_period("Profitability", 10.00, 20.00, -2.0, +2.0, start, int0);
    	p.set_array_values_on_period("Profitability", 20.00, 25.00, -2.0, +2.0, int0, int1);
    	p.set_array_values_on_period("Profitability", 25.00, 20.00, -2.0, +2.0, int1, int2);
    	p.set_array_values_on_period("Profitability", 20.00, 25.00, -2.0, +2.0, int2, int3);
    	p.set_array_values_on_period("Profitability", 25.00, 30.00, -2.0, +2.0, int3, int4);
    	p.set_array_values_on_period("Profitability", 32.00, 30.00, -2.0, +2.0, int4, int5);
    	p.set_array_values_on_period("Profitability", 32.00, 35.00, -2.0, +2.0, int5, end);
    	
    	p.set_array_values_on_period("MediaCost", 07.00, 12.00, -2.0, +2.0, start, int0);
    	p.set_array_values_on_period("MediaCost", 12.00, 15.00, -2.0, +2.0, int0, int1);
    	p.set_array_values_on_period("MediaCost", 15.00, 17.00, -2.0, +2.0, int1, int2);
    	p.set_array_values_on_period("MediaCost", 17.00, 18.00, -2.0, +2.0, int2, int3);
    	p.set_array_values_on_period("MediaCost", 18.00, 15.00, -2.0, +2.0, int3, int4);
    	p.set_array_values_on_period("MediaCost", 15.00, 19.00, -2.0, +2.0, int4, int5);
    	p.set_array_values_on_period("MediaCost", 18.00, 20.00, -2.0, +2.0, int5, end);
    	
    	p.set_array_values_on_period("EstimatedBalance_1", 800.0, 1000.0, -2.0, +2.0, start, int0);
    	p.set_array_values_on_period("EstimatedBalance_1", 1000.0, 800.0, -2.0, +2.0, int0, int1);
    	p.set_array_values_on_period("EstimatedBalance_1", 800.0, 700.0, -2.0, +2.0, int1, int2);
    	p.set_array_values_on_period("EstimatedBalance_1", 700.0, 750.0, -2.0, +2.0, int2, int3);
    	p.set_array_values_on_period("EstimatedBalance_1", 750.0, 1000.0, -2.0, +2.0, int3, int4);
    	p.set_array_values_on_period("EstimatedBalance_1", 1000.0, 1200.0, -2.0, +2.0, int4, int5);
    	p.set_array_values_on_period("EstimatedBalance_1", 1200.0, 1400.0, -2.0, +2.0, int5, end);
    	
    	
	}
		
	static void update_ContentCatGroup(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime int0 =    new DateTime(2013, 6, 30, 0, 0);
    	DateTime int1 =    new DateTime(2013, 7, 30, 0, 0);
    	DateTime int2 =    new DateTime(2013, 8, 30, 0, 0);
    	DateTime int3 =    new DateTime(2013, 9, 30, 0, 0);
    	DateTime int4 =    new DateTime(2013, 10, 30, 0, 0);
    	DateTime int5 =    new DateTime(2013, 11, 30, 0, 0);
    	DateTime int6 =    new DateTime(2013, 12, 30, 0, 0);
    	DateTime int7 =    new DateTime(2014, 1, 30, 0, 0);
    	DateTime int8 =    new DateTime(2014, 2, 28, 0, 0);
    	DateTime int9 =    new DateTime(2014, 3, 30, 0, 0);
    	DateTime int10 =   new DateTime(2014, 4, 30, 0, 0, 0);
    	DateTime end =     new DateTime(2014, 5, 30, 0, 0, 0);
    	
    	
		Tools.percentage_checksum(db.content_category_group_percentage);
		
		p.update_ContentCatGroup(db, start, int0);
		p.update_ContentCatGroup(db, int0, int1);
		p.update_ContentCatGroup(db, int1, int2);
		p.update_ContentCatGroup(db, int2, int3);
		p.update_ContentCatGroup(db, int3, int4);
		p.update_ContentCatGroup(db, int4, int5);
		p.update_ContentCatGroup(db, int5, int6);
		p.update_ContentCatGroup(db, int6, int7);
		p.update_ContentCatGroup(db, int7, int8);
		p.update_ContentCatGroup(db, int8, int9);
		p.update_ContentCatGroup(db, int9, int10);
		p.update_ContentCatGroup(db, int10, end);
	}
	
	static void update_ContentCatGroup_weekly(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 5, 30, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
		Tools.percentage_checksum(db.content_category_group_percentage);
		
		int counter = 0;
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			p.update_ContentCatGroup(db, intermediate_dates.get(i), intermediate_dates.get(i+1));
			counter++;
		}
		System.out.println("> updated content category group for "+counter+" periods");
	}
	
	static void update_ContentCategory_weekly(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 6, 30, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
		Tools.percentage_checksum(db.content_category_percentage);
		
		int counter = 0;
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			p.update_ContentCategory(db, intermediate_dates.get(i), intermediate_dates.get(i+1));
			counter++;
		}
		
		System.out.println("> updated content category for "+counter+" periods");
		
		// once Content Categories have been updated, mobile fields need to be updated accordingly
		update_mobile_weekly(db,p);
		// once Content Categories have been updated, PathFinder and DFA Accounts needs to be re-distributed accordingly
		update_pf_dfa_amounts_distribution_weekly(db,p);
	}
	
	static void update_mobile_weekly(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 01, 0, 0);
    	DateTime end =     new DateTime(2014, 7, 01, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
		Tools.percentage_checksum(db.DeviceOS_smartphone_percentage);
		Tools.percentage_checksum(db.DeviceOS_tablet_percentage);
		Tools.percentage_checksum(db.DeviceViewType_percentage);
		Tools.percentage_checksum(db.MobileDevice_percentage);
		
		int counter = 0;
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			counter += p.update_mobile(db, intermediate_dates.get(i), intermediate_dates.get(i+1));
			
		}
		System.out.println("> updated mobile content for "+counter+" records");
	}
	
	static void update_products_weekly(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 01, 0, 0);
    	DateTime end =     new DateTime(2014, 7, 01, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
    	for (int i=0; i<UserPreferences.ProductType_percentage.size();i++)
		{
    		Tools.percentage_checksum(UserPreferences.ProductType_percentage.get(i));
		}
		int counter = 0;
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			counter += p.update_products(db, intermediate_dates.get(i), intermediate_dates.get(i+1));
			
		}
		System.out.println("> updated products for "+counter+" records");
	}
	
	static void update_pf_dfa_amounts_distribution_weekly(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 01, 0, 0);
    	DateTime end =     new DateTime(2014, 7, 01, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
    	for (int i=0; i<UserPreferences.ProductType_percentage.size();i++)
		{
    		Tools.percentage_checksum(UserPreferences.ProductType_percentage.get(i));
		}
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			p.update_pf_dfa_amounts_distribution(db, intermediate_dates.get(i), intermediate_dates.get(i+1));
			
		}
		System.out.println("> updated pf and dfa accounts for records");
	}
	
	static void count_rows_weekly(UserPreferences db, Records p)
	{
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 6, 30, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
		//Tools.percentage_checksum(db.content_category_percentage);
		
		//int counter = 0;
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			p.count_rows(db, intermediate_dates.get(i), intermediate_dates.get(i+1));
			//counter++;
		}
		//System.out.println("> updated "+counter+" periods");
	}
	
	static void update_ROAS_byPercentage_weekly(UserPreferences db, Records p, String SiteContentCategory, double fraction, boolean add)
	{
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 5, 30, 0, 0, 0);
    	
    	ArrayList<DateTime> intermediate_dates = new ArrayList<DateTime>();
    	
    	intermediate_dates = sundays_generator(start,end);
    	
		//Tools.percentage_checksum(db.content_category_percentage);
		
		int counter = 0;
		
		for (int i=0; i<intermediate_dates.size()-1;i++)
		{
			p.update_ROAS_by_ContentCategory(db, intermediate_dates.get(i), intermediate_dates.get(i+1), SiteContentCategory, fraction, add);
			counter++;
		}
		System.out.println("> updated "+counter+" periods");
	}
	
	//static void update_ 
	
	static ArrayList<DateTime> sundays_generator(DateTime start, DateTime end)
	{	
		ArrayList<DateTime> dates = new ArrayList<DateTime>();
		
		LocalDate startDate = new LocalDate(start);
		LocalDate endDate =   new LocalDate(end);
		
		LocalDate thisSunday = startDate.withDayOfWeek(DateTimeConstants.SUNDAY);

		if (startDate.isAfter(thisSunday)) 
		{
			startDate = thisSunday.plusWeeks(1); // start on next Sunday
		} 
		else 
		{
			startDate = thisSunday; // start on this Sunday
		}

		while (startDate.isBefore(endDate)) 
		{
			//System.out.println(startDate);
			startDate = startDate.plusWeeks(1);
			dates.add(startDate.toDateTimeAtStartOfDay());
		}
		
		for (int i=0; i<dates.size();i++)
		{
			//System.out.println(dates.get(i));
		}
		return dates;

		
	}
	
    public static void resolve_inconsistencies(Records p)
    {
    	p.resolve_ccg_strategy_inconsistency();
		p.resolve_sites_inconsistency();
		p.resolve_placements_inconsistency();
		p.resolve_compaingn_name_inconsistency();
		Tools.fix_IPad(p);
		Tools.move_frontpage_to_awareness(p);
		p.resolve_products();
    }
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception 
    {
    	try 
        {
            UserPreferences db = new UserPreferences();
            db.dates_selector();

            //Tools.percentage_checksum(DAO.cc_perc_sc2);
            
            Records p = new Records();
            p.create_population(); 
            
            //update_Profitability(p);
            
            //p.info();
            
            //update_ROAS(p);
            
            update_ContentCategory_weekly(db,p);
            //update_mobile_weekly(db,p);
            
            //p.resolve_inconsistency();
            
            //update_ROAS_byPercentage_weekly(db,p,"Social Retargeting",0.2,false);
            //update_ROAS_byPercentage_weekly(db,p,"Retargeting",0.38,false);
            //p.update_Accounts_number();
            //p.update_clicks_impressions();
            //count_rows_weekly(db,p);
            
            //update_products_weekly(db,p);
            //p.info();
            //p.update_Accounts_number();
            //p.info();
            //update_pf_dfa_amounts_distribution_weekly(db,p);
            //p.info();
            
            resolve_inconsistencies(p);
            ///p.info();
            p.update_database();
            
            
            //Tools.percentage_checksum(db.content_category_percentage);
            //Tools.percentage_checksum(db.cc_perc_sc2);
            //Tools.percentage_checksum(db.content_category_percentage_specialCase);
        } 
        //catch (IOException e) {e.printStackTrace();} 
        catch (ApplicationException e) {
            System.out.println(e.getMessage());
            System.out.println(e);
        }
        
    }
}
