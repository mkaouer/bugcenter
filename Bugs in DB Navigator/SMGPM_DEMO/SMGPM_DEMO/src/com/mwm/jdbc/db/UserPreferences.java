package com.mwm.jdbc.db;
//java SQL
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
// java utilities
import java.util.ArrayList;
import java.util.Date;






// local import
import com.mwm.jdbc.db.ConnectionFactory;
import com.mwm.jdbc.db.Tools;
import com.mwm.jdbc.db.ApplicationException;
 
public class UserPreferences 
{
	// Connection Parameters
	
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    
    // Global Parameters for Scripts execution
    
 	final public static int max_queried_rows = 100;
 	//final public static String tableName = new String("Draft"); // mini-test DB
 	//final public static String tableName = new String("RPT_SPBPOD2"); // real DB
 	final public static String tableName = new String("RPT_SPBPOD"); // final prototype DB
 	final public static String indexing_date = new String("ActivityDate");
 	// DB fields
 	public static ArrayList<String> content_category ;
 	public static ArrayList<ArrayList<String>> cc_to_ccg ;
 	public static ArrayList<ArrayList<String>> ccg_to_strategy ;
 	public static ArrayList<String> content_category_group ;
 	public static ArrayList<Double> content_category_percentage ;
 	public static ArrayList<Double> content_category_percentage_specialCase ;
 	public static ArrayList<Double> cc_perc_sc2 ;
 	public static ArrayList<Double> content_category_group_percentage ;
 	
 	public  ArrayList<String> extracted_content_category ;
 	public  ArrayList<String> extracted_content_category_group ;
 	public  ArrayList<Double> extracted_content_category_percentage ;
 	public  ArrayList<Double> extracted_content_category_group_percentage ;
 	//final public static String indexing_date = new String("year_month");
    
 	public  static ArrayList<String> MobileDevice ;
 	public  static ArrayList<String> DeviceViewType ;
 	
 	public  static ArrayList<Double> MobileDevice_percentage ;
 	public  static ArrayList<Double> DeviceViewType_percentage ;
 	
 	public  static ArrayList<String> DeviceOS_smartphone ;
 	public  static ArrayList<Double> DeviceOS_smartphone_percentage ;
 	public  static ArrayList<String> DeviceOS_tablet ;
 	public  static ArrayList<Double> DeviceOS_tablet_percentage ;
 	
 	public  static ArrayList<String> Mobile_keywords ;
 	
 	public  static ArrayList<String> ProductType ;
 	public  static ArrayList<ArrayList<Double>> ProductType_percentage ;
 	public  static ArrayList<Double> a,b,c,d,e,f,s1,s2,s3,s4 ;
 	public  static ArrayList<String> Product ;
 	public  static ArrayList<String> TotalProduct ;
 	
 	public  static ArrayList<Double> pf_variation_factor ;
 	public  static ArrayList<Double> dfa_variation_factor ;
 	
 	public static ArrayList<String> ccg_abbreviation ;
 	public static ArrayList<String> cc_abbreviation;
 	public static ArrayList<String> banners_sizes;
    
    public UserPreferences() 
    { 
    	content_category_group = new ArrayList<String>();   content_category_group_percentage = new ArrayList<Double>();  ccg_to_strategy = new ArrayList<ArrayList<String>>(); ArrayList<String> temp; ccg_abbreviation = new ArrayList<String>();
    	content_category_group.add("Display Mobile");       content_category_group_percentage.add(0.02); temp = new ArrayList<String>(); temp.add("Awareness"); temp.add("Engagement"); ccg_to_strategy.add(temp);ccg_abbreviation.add("DM");
    	content_category_group.add("Paid Search");          content_category_group_percentage.add(0.20); temp = new ArrayList<String>(); temp.add("Conversion"); ccg_to_strategy.add(temp);ccg_abbreviation.add("S");
    	content_category_group.add("Paid Search Mobile");   content_category_group_percentage.add(0.05); temp = new ArrayList<String>(); temp.add("Awareness"); ccg_to_strategy.add(temp);ccg_abbreviation.add("SM");
    	content_category_group.add("Display Rate Tables");  content_category_group_percentage.add(0.17); temp = new ArrayList<String>(); temp.add("Conversion"); ccg_to_strategy.add(temp);ccg_abbreviation.add("DRT");
    	content_category_group.add("Display Mobile Video"); content_category_group_percentage.add(0.02); temp = new ArrayList<String>(); temp.add("Awareness"); ccg_to_strategy.add(temp);ccg_abbreviation.add("DMV");
    	content_category_group.add("Affiliate Networks");   content_category_group_percentage.add(0.04); temp = new ArrayList<String>(); temp.add("Conversion"); ccg_to_strategy.add(temp);ccg_abbreviation.add("A");
    	content_category_group.add("Organic Search");       content_category_group_percentage.add(0.15); temp = new ArrayList<String>(); temp.add("Awareness"); ccg_to_strategy.add(temp);ccg_abbreviation.add("OrganicSearch");
    	content_category_group.add("Display"); 				content_category_group_percentage.add(0.30); temp = new ArrayList<String>(); temp.add("Awareness"); temp.add("Engagement"); temp.add("Conversion"); ccg_to_strategy.add(temp);ccg_abbreviation.add("D");
    	content_category_group.add("Display Video"); 		content_category_group_percentage.add(0.05); temp = new ArrayList<String>(); temp.add("Awareness"); temp.add("Engagement"); ccg_to_strategy.add(temp);ccg_abbreviation.add("DV");
    	
    	Mobile_keywords = new ArrayList<String>(); Mobile_keywords.add("Display Mobile Video");Mobile_keywords.add("Paid Search Mobile");Mobile_keywords.add("Display Mobile");
    	
    	content_category = new ArrayList<String>();	content_category_percentage = new ArrayList<Double>();  content_category_percentage_specialCase = new ArrayList<Double>(); cc_to_ccg = new ArrayList<ArrayList<String>>(); cc_perc_sc2 = new ArrayList<Double>();pf_variation_factor=new ArrayList<Double>();dfa_variation_factor=new ArrayList<Double>();cc_abbreviation = new ArrayList<String>();
    	
    	content_category.add("Affiliate Marketing"); pf_variation_factor.add(0.020);dfa_variation_factor.add(0.009);content_category_percentage.add(0.01);content_category_percentage_specialCase.add(0.01);cc_perc_sc2.add(0.01);temp = new ArrayList<String>(); temp.add("Affiliate Networks"); cc_to_ccg.add(temp);cc_abbreviation.add("AFF");
    	
    	content_category.add("Acquisition");		 pf_variation_factor.add(0.026);dfa_variation_factor.add(0.006);content_category_percentage.add(0.03);content_category_percentage_specialCase.add(0.01);cc_perc_sc2.add(0.01);temp = new ArrayList<String>(); temp.add("Display"); cc_to_ccg.add(temp);cc_abbreviation.add("CPA");
    	content_category.add("Ad Network");			 pf_variation_factor.add(-0.041);dfa_variation_factor.add(-0.03);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.01);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display");temp.add("Display Mobile"); cc_to_ccg.add(temp);cc_abbreviation.add("CPA");
    	content_category.add("BankrateFin");		 pf_variation_factor.add(0.509);dfa_variation_factor.add(0.205);content_category_percentage.add(0.05);content_category_percentage_specialCase.add(0.05);cc_perc_sc2.add(0.05);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("BankrateSpecial");	 pf_variation_factor.add(0.250);dfa_variation_factor.add(0.050);content_category_percentage.add(0.05);content_category_percentage_specialCase.add(0.05);cc_perc_sc2.add(0.04);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Behavioral Targeting");pf_variation_factor.add(-0.012);dfa_variation_factor.add(-0.005);content_category_percentage.add(0.03);content_category_percentage_specialCase.add(0.01);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display");temp.add("Display Mobile");temp.add("Display Mobile Video");cc_to_ccg.add(temp);
    	content_category.add("Co-branded");			 pf_variation_factor.add(-0.129);dfa_variation_factor.add(-0.311);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Digital Video");		 pf_variation_factor.add(-0.224);dfa_variation_factor.add(-0.415);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Email");				 pf_variation_factor.add(-0.555);dfa_variation_factor.add(-0.038);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Enhanced Targeting");	 pf_variation_factor.add(0.040);dfa_variation_factor.add(0.331);content_category_percentage.add(0.08);content_category_percentage_specialCase.add(0.10);cc_perc_sc2.add(0.10);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display Mobile");temp.add("Display Mobile Video");temp.add("Display Video");temp.add("Display Video");cc_to_ccg.add(temp);
    	content_category.add("Finance");			 pf_variation_factor.add(0.550);dfa_variation_factor.add(0.210);content_category_percentage.add(0.09);content_category_percentage_specialCase.add(0.05);cc_perc_sc2.add(0.05);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display Mobile");temp.add("Display Mobile Video");temp.add("Display Video");temp.add("Display Video");cc_to_ccg.add(temp);
    	content_category.add("FinanceNoImp");		 pf_variation_factor.add(0.050);dfa_variation_factor.add(0.010);content_category_percentage.add(0.04);content_category_percentage_specialCase.add(0.03);cc_perc_sc2.add(0.01);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Frontpage");			 pf_variation_factor.add(0.130);dfa_variation_factor.add(0.015);content_category_percentage.add(0.03);content_category_percentage_specialCase.add(0.12);cc_perc_sc2.add(0.12);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display");temp.add("Display Mobile");temp.add("Display Video");cc_to_ccg.add(temp);
    	content_category.add("Lifestyle");			 pf_variation_factor.add(0.093);dfa_variation_factor.add(0.002);content_category_percentage.add(0.04);content_category_percentage_specialCase.add(0.04);cc_perc_sc2.add(0.04);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display Mobile");temp.add("Display Video");temp.add("Display Video");cc_to_ccg.add(temp);
    	content_category.add("News");				 pf_variation_factor.add(-0.530);dfa_variation_factor.add(-0.011);content_category_percentage.add(0.06);content_category_percentage_specialCase.add(0.04);cc_perc_sc2.add(0.03);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display Mobile");temp.add("Display Mobile Video");temp.add("Display Video");temp.add("Display Video");cc_to_ccg.add(temp);
    	content_category.add("Portal Homepage");	 pf_variation_factor.add(0.037);dfa_variation_factor.add(0.0220);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.03);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Retirement");			 pf_variation_factor.add(-0.410);dfa_variation_factor.add(-0.039);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.03);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Sports");				 pf_variation_factor.add(-0.250);dfa_variation_factor.add(-0.175);content_category_percentage.add(0.01);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.03);temp = new ArrayList<String>(); temp.add("Display");temp.add("Display Video");cc_to_ccg.add(temp);
    	
    	content_category.add("Mobile");				 pf_variation_factor.add(-0.408);dfa_variation_factor.add(-0.255);content_category_percentage.add(0.03);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Display Mobile");temp.add("Paid Search Mobile");cc_to_ccg.add(temp);
    	
    	content_category.add("Organic Search");		 pf_variation_factor.add(-0.310);dfa_variation_factor.add(-0.144);content_category_percentage.add(0.05);content_category_percentage_specialCase.add(0.03);cc_perc_sc2.add(0.03);temp = new ArrayList<String>(); temp.add("Organic Search");cc_to_ccg.add(temp);

    	content_category.add("Ipad");				 pf_variation_factor.add(-0.206);dfa_variation_factor.add(-0.505);content_category_percentage.add(0.02);content_category_percentage_specialCase.add(0.02);cc_perc_sc2.add(0.02);temp = new ArrayList<String>(); temp.add("Paid Search");cc_to_ccg.add(temp);
    	content_category.add("Paid Search");		 pf_variation_factor.add(0.629);dfa_variation_factor.add(0.261);content_category_percentage.add(0.10);content_category_percentage_specialCase.add(0.12);cc_perc_sc2.add(0.12);temp = new ArrayList<String>(); temp.add("Paid Search");cc_to_ccg.add(temp);
    	
    	content_category.add("Rate Tables");		 pf_variation_factor.add(0.182);dfa_variation_factor.add(0.770);content_category_percentage.add(0.05);content_category_percentage_specialCase.add(0.04);cc_perc_sc2.add(0.04);temp = new ArrayList<String>(); temp.add("Display Rate Tables");cc_to_ccg.add(temp);
    	
    	content_category.add("Retargeting");		 pf_variation_factor.add(0.223);dfa_variation_factor.add(0.012);content_category_percentage.add(0.06);content_category_percentage_specialCase.add(0.08);cc_perc_sc2.add(0.07);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	content_category.add("Social Retargeting");	 pf_variation_factor.add(0.336);dfa_variation_factor.add(0.025);content_category_percentage.add(0.05);content_category_percentage_specialCase.add(0.05);cc_perc_sc2.add(0.05);temp = new ArrayList<String>(); temp.add("Display");cc_to_ccg.add(temp);
    	
    	MobileDevice = new ArrayList<String>(); MobileDevice_percentage = new ArrayList<Double>();
    	MobileDevice.add("Smartphone"); MobileDevice_percentage.add(0.6);
    	MobileDevice.add("Tablet") ;    MobileDevice_percentage.add(0.4);
    	
    	DeviceViewType = new ArrayList<String>(); DeviceViewType_percentage = new ArrayList<Double>();
    	DeviceViewType.add("App") ; DeviceViewType_percentage.add(0.3);
    	DeviceViewType.add("Web") ; DeviceViewType_percentage.add(0.7);
    	
    	DeviceOS_smartphone = new ArrayList<String>(); DeviceOS_smartphone_percentage = new ArrayList<Double>();
    	DeviceOS_smartphone.add("Iphone") ;  DeviceOS_smartphone_percentage.add(0.4);
    	DeviceOS_smartphone.add("Android") ; DeviceOS_smartphone_percentage.add(0.6);
    	
    	DeviceOS_tablet = new ArrayList<String>(); DeviceOS_tablet_percentage = new ArrayList<Double>();
    	DeviceOS_tablet.add("Ipad") ;    DeviceOS_tablet_percentage.add(0.7);
    	DeviceOS_tablet.add("Android") ; DeviceOS_tablet_percentage.add(0.3);
     	
    	ProductType  = new ArrayList<String>();				ProductType_percentage  = new ArrayList<ArrayList<Double>>(); a=new ArrayList<Double>();b=new ArrayList<Double>();c=new ArrayList<Double>();d=new ArrayList<Double>();e=new ArrayList<Double>();f=new ArrayList<Double>();
    	Product  = new ArrayList<String>();	TotalProduct  = new ArrayList<String>();	
    	ProductType.add("Online Savings");					Product.add("Savings");TotalProduct.add("Online Savings");		a.add(0.20);b.add(0.21);c.add(0.23);d.add(0.24);e.add(0.26);f.add(0.27);
    	ProductType.add("Money Market");					Product.add("Savings");TotalProduct.add("Money Market");		a.add(0.13);b.add(0.12);c.add(0.13);d.add(0.14);e.add(0.13);f.add(0.11);
    	ProductType.add("Interest Checking");				Product.add("Checking");TotalProduct.add("Interest Checking");	a.add(0.22);b.add(0.24);c.add(0.23);d.add(0.20);e.add(0.18);f.add(0.20);
    	ProductType.add("12 Month CD");						Product.add("Savings");TotalProduct.add("CD");					a.add(0.09);b.add(0.08);c.add(0.09);d.add(0.08);e.add(0.09);f.add(0.07);
    	
    	ProductType.add("5 Year CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.05);b.add(0.04);c.add(0.05);d.add(0.04);e.add(0.03);f.add(0.02);
    	ProductType.add("4 Year CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.04);b.add(0.06);c.add(0.04);d.add(0.03);e.add(0.04);f.add(0.02);
    	ProductType.add("3 Year CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.03);b.add(0.02);c.add(0.02);d.add(0.03);e.add(0.03);f.add(0.04);
    	ProductType.add("2 Year CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.03);b.add(0.02);c.add(0.02);d.add(0.04);e.add(0.03);f.add(0.02);
    	
    	ProductType.add("Online Savings IRA");				Product.add("IRA");TotalProduct.add("IRA");	a.add(0.02);b.add(0.01);c.add(0.01);d.add(0.01);e.add(0.02);f.add(0.01);
    	ProductType.add("High Yield 12M CD IRA");			Product.add("IRA");TotalProduct.add("IRA");	a.add(0.01);b.add(0.02);c.add(0.01);d.add(0.02);e.add(0.02);f.add(0.02);
    	ProductType.add("No Penalty CD");					Product.add("Savings");TotalProduct.add("CD");	a.add(0.02);b.add(0.01);c.add(0.01);d.add(0.01);e.add(0.01);f.add(0.03);
    	
    	ProductType.add("IRA High Yield 5-Year CD");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.02);b.add(0.02);c.add(0.01);d.add(0.02);e.add(0.01);f.add(0.02);
    	ProductType.add("IRA Raise Your Rate 4-Year CD");	Product.add("IRA");TotalProduct.add("IRA");	a.add(0.02);b.add(0.01);c.add(0.01);d.add(0.01);e.add(0.01);f.add(0.01);
    	ProductType.add("IRA High Yield 3-Year CD");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.01);b.add(0.02);c.add(0.02);d.add(0.01);e.add(0.02);f.add(0.01);
    	ProductType.add("Raise Your Rate 2Y CD IRA");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.01);b.add(0.01);c.add(0.01);d.add(0.02);e.add(0.01);f.add(0.02);
    	
    	ProductType.add("3 Month CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.02);b.add(0.01);c.add(0.02);d.add(0.01);e.add(0.01);f.add(0.01);
    	ProductType.add("6 Month CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.01);b.add(0.02);c.add(0.01);d.add(0.01);e.add(0.01);f.add(0.02);
    	ProductType.add("9 Month CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.01);b.add(0.01);c.add(0.02);d.add(0.01);e.add(0.02);f.add(0.01);
    	
    	ProductType.add("18 Month CD");						Product.add("Savings");TotalProduct.add("CD");	a.add(0.01);b.add(0.02);c.add(0.01);d.add(0.01);e.add(0.02);f.add(0.03);
    	
    	ProductType.add("IRA High Yield 3-month CD");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.02);b.add(0.01);c.add(0.01);d.add(0.02);e.add(0.01);f.add(0.01);
    	ProductType.add("IRA High Yield 6-month CD");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.01);b.add(0.02);c.add(0.01);d.add(0.01);e.add(0.01);f.add(0.02);
    	ProductType.add("IRA High Yield 9-month CD");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.01);b.add(0.01);c.add(0.02);d.add(0.01);e.add(0.01);f.add(0.02);
    	ProductType.add("IRA High Yield 18-month CD");		Product.add("IRA");TotalProduct.add("IRA");	a.add(0.01);b.add(0.01);c.add(0.01);d.add(0.02);e.add(0.02);f.add(0.01);
    	
    	ProductType_percentage.add(a);ProductType_percentage.add(b);ProductType_percentage.add(c);ProductType_percentage.add(d);ProductType_percentage.add(e);ProductType_percentage.add(f);
    	banners_sizes = new ArrayList<String>(); banners_sizes.add("(160x600)");banners_sizes.add("(300x600)");banners_sizes.add("(1x1)");banners_sizes.add("(728x90)");banners_sizes.add("(300x250)");banners_sizes.add("(320x50)");banners_sizes.add("(970x66)");banners_sizes.add("(800x600)");banners_sizes.add("(970x50)");banners_sizes.add("(790x46)");banners_sizes.add("(640x460)");banners_sizes.add("(150x200)");banners_sizes.add("(200x150)");banners_sizes.add("(100x150)");banners_sizes.add("(150x100)");banners_sizes.add("(250x50)");
    }
    
    @SuppressWarnings("finally")
	public ArrayList<String> extract_field_unique_values(String field)
	{
    	ArrayList<String> content = null ;
    	try 
        {
			ArrayList<String> data = new ArrayList<String> (this.column_selector_string(field));
			content = new ArrayList<String>(Tools.duplicates_remover(data));
			//return content;
        }
		catch (ApplicationException e) 
		{
            System.out.println(e.getMessage());
            System.out.println(e);
        }
		finally 
        {
            return content;
        }	
	}
    @SuppressWarnings("finally")
	public ArrayList<Double> percentage_calculator(ArrayList<String> content, String field)
	{
    	ArrayList<Double> percentage = null;
    	try 
        {
			percentage = new ArrayList<Double>();
			ArrayList<String> data = new ArrayList<String> (this.column_selector_string(field));
			int total_occurrences = 0 ;
			// determining percentages
			for(int i=0;i<content.size();i++)
	        {
	    		int occurrence = 0 ;
	    		
	    		for(int j=0;j<data.size();j++)
		        {
	    			if (content.get(i).equals(data.get(j)))
	    			{
	    				occurrence++;
	    			}
		        }
	    		//System.out.println(" dividing "+occurrence+" by total of "+data.size());
	    		total_occurrences += occurrence ;
	    		if (occurrence != 0)
	    		{
	    			percentage.add( (Double)((double)occurrence)/data.size() );
	    		}
	    		else
	    		{
	    			percentage.add(0.0);
	    		}
	    			
	        }
			//System.out.println(" divided total "+total_occurrences+" occurrences by total of "+data.size());
        }
		catch (ApplicationException e) 
		{
            System.out.println(e.getMessage());
            System.out.println(e);
        }
    	finally 
        {
    		System.out.println(" content headers size "+content.size()+" percentage size "+percentage.size());
    		return percentage;
        }		
	}
    
    @SuppressWarnings("finally")
	public ArrayList<Double> percentage_calculator_from_array(ArrayList<String> content, ArrayList<String> data, String field)
	{
    	ArrayList<Double> percentage = null;
    	try 
        {
			percentage = new ArrayList<Double>();
			//ArrayList<String> data = new ArrayList<String> (this.column_selector_string(field));
			int total_occurrences = 0 ;
			// determining percentages
			for(int i=0;i<content.size();i++)
	        {
	    		int occurrence = 0 ;
	    		
	    		for(int j=0;j<data.size();j++)
		        {
	    			if (content.get(i).equals(data.get(j)))
	    			{
	    				occurrence++;
	    			}
		        }
	    		//System.out.println(" dividing "+occurrence+" by total of "+data.size());
	    		total_occurrences += occurrence ;
	    		if (occurrence != 0)
	    		{
	    			percentage.add( (Double)((double)occurrence)/data.size() );
	    		}
	    		else
	    		{
	    			percentage.add(0.0);
	    		}
	    			
	        }
			//System.out.println(" divided total "+total_occurrences+" occurrences by total of "+data.size());
        }
    	finally 
        {
    		//System.out.println(" percentage size "+percentage.size());
    		return percentage;
        }		
	}
 
 
    @SuppressWarnings("finally")
	public int rows_number(String tableName) throws ApplicationException 
    {
        String query = "SELECT Count(*) as total FROM " + tableName;
        int rows_number = 0 ;
        try 
        {
            connection = ConnectionFactory.getConnection();
            statement = connection.createStatement();
            resultSet =statement.executeQuery(query);
            
            while(resultSet .next())
            {
            	rows_number = resultSet.getInt("total");
            }
            
            SQLWarning warning = statement.getWarnings();
            if (warning != null)
                throw new ApplicationException(warning.getMessage());
        } 
        catch (SQLException e) 
        {
            ApplicationException exception = new ApplicationException(e.getMessage(), e);
            throw exception;
        } 
        finally 
        {
            Tools.close(statement);
            Tools.close(connection);
            Tools.close(resultSet);
            System.out.println("Query Successfully Executed, size of "+tableName+" is "+rows_number);
            return rows_number;
        }
    }
        
        @SuppressWarnings("finally")
		public ArrayList<Double> column_selector_double(String columnName) throws ApplicationException 
        {
            String query = "SELECT "+columnName+" as selected FROM " + tableName;
            ArrayList<Double> queried_values = new ArrayList<Double>();
            try 
            {
                connection = ConnectionFactory.getConnection();
                statement = connection.createStatement();
                resultSet =statement.executeQuery(query);
                
                while(resultSet .next())
                {
                	queried_values.add(resultSet.getDouble("selected"));
                }
                
                SQLWarning warning = statement.getWarnings();
                if (warning != null)
                    throw new ApplicationException(warning.getMessage());
            } 
            catch (SQLException e) 
            {
                ApplicationException exception = new ApplicationException(e.getMessage(), e);
                throw exception;
            } 
            finally 
            {
                Tools.close(statement);
                Tools.close(connection);
                Tools.close(resultSet);
                System.out.println("Query Successfully Executed, successfully got "+queried_values.size()+" of column "+columnName);
                return queried_values;
            }
        }
        
        @SuppressWarnings("finally")
		public ArrayList<String> column_selector_string(String columnName) throws ApplicationException 
        {
            String query = "SELECT "+columnName+" as selected FROM " + tableName;
            ArrayList<String> queried_values = new ArrayList<String>();
            try 
            {
                connection = ConnectionFactory.getConnection();
                statement = connection.createStatement();
                resultSet =statement.executeQuery(query);
                
                while(resultSet .next())
                {
                	queried_values.add(resultSet.getString("selected"));
                }
                
                SQLWarning warning = statement.getWarnings();
                if (warning != null)
                    throw new ApplicationException(warning.getMessage());
            } 
            catch (SQLException e) 
            {
                ApplicationException exception = new ApplicationException(e.getMessage(), e);
                throw exception;
            } 
            finally 
            {
                Tools.close(statement);
                Tools.close(connection);
                Tools.close(resultSet);
                //System.out.println("Query Successfully Executed, successfully got "+queried_values.size()+" of column "+columnName);
                return queried_values;
            }
        } 
        
        
        @SuppressWarnings("finally")
		public ArrayList<Date> dates_selector() throws ApplicationException 
        {
            String query = "SELECT "+indexing_date+" as selected FROM " + tableName;
            ArrayList<Date> queried_values = new ArrayList<Date>();
            try 
            {
                connection = ConnectionFactory.getConnection();
                statement = connection.createStatement();
                resultSet =statement.executeQuery(query);
                
                while(resultSet .next())
                {
                	java.sql.Date dbSqlDate = resultSet.getDate("selected");
                	java.util.Date dbSqlDateConverted = new java.util.Date(dbSqlDate.getTime());
                	queried_values.add(dbSqlDateConverted);
                }
                
                SQLWarning warning = statement.getWarnings();
                if (warning != null)
                    throw new ApplicationException(warning.getMessage());
            } 
            catch (SQLException e) 
            {
                ApplicationException exception = new ApplicationException(e.getMessage(), e);
                throw exception;
            } 
            finally 
            {
                Tools.close(statement);
                Tools.close(connection);
                Tools.close(resultSet);
                //System.out.println("Query Successfully Executed, successfully got "+queried_values.size()+" of dates from "+indexing_date);
                // Removing duplicates
                //queried_values = new ArrayList<Date>(Tools.duplicates_remover(queried_values));
                return queried_values;
            }
        }      
}