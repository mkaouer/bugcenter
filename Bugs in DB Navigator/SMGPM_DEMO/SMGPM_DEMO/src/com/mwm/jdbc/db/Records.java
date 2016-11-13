package com.mwm.jdbc.db;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.PreparedStatement;
// java utilities
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Collections;














import java.util.Random;

import org.joda.time.DateTime;











//local import
import com.mwm.jdbc.db.ConnectionFactory;
import com.mwm.jdbc.db.Tools;
import com.mwm.jdbc.db.ApplicationException;

public class Records 
{
	ArrayList<Record> records;
	Strategy awareness;
	Strategy engagement;
	Strategy conversion;
	
	ArrayList<ArrayList<String>> consistency_matrix;
	
	static ArrayList<String> fields; 
	static ArrayList<String> paid_search;
	static ArrayList<String> retargeting;
	static ArrayList<String> front_page;
	static ArrayList<String> bankrate;
	
	static ArrayList<String> adNetwork;
	static ArrayList<String> Affiliate_Marketing;
	static ArrayList<String> behavioral_Targeting;
	static ArrayList<String> co_branded;
	static ArrayList<String> enhanced_Targeting;
	static ArrayList<String> acquisition;
	static ArrayList<String> finance;
	static ArrayList<String> digital_video;
	static ArrayList<String> email;
	
	static ArrayList<String> organic_search;
	static ArrayList<String> rate_tables;
	static ArrayList<String> news;
	static ArrayList<String> mobile;
	static ArrayList<String> Ipad;
	static ArrayList<String> lifestyle;
	static ArrayList<String> financeNoImp;
	
	static ArrayList<String> portal_homepage;
	static ArrayList<String> retirement;
	static ArrayList<String> social_retargeting;
	static ArrayList<String> sports;
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
 	final public String NULL = new String("NULL");
 	
 	public static void fill_fields()
 	{
 		fields = new ArrayList<String>();
 		fields.add("ID");
		fields.add("ActivityDate");
		fields.add("DFA_Site_Name");
		fields.add("Placement_Group_Strategy");
		fields.add("SiteContentCategory");
		fields.add("ContentCatGroup");
		fields.add("Product");
		fields.add("ProductType");
		fields.add("EV_1");	
		fields.add("MediaCost");
		fields.add("EstimatedBalance_1");
		fields.add("DFA_Accounts");
		fields.add("ActivityRevenue_1");
		fields.add("DFASumImps");
		fields.add("DFASumClicks");
		fields.add("Attribution");
		fields.add("MobileDevice");
		fields.add("DeviceViewType");
		fields.add("DeviceOS");
		fields.add("TotalProduct");
		
		paid_search  = new ArrayList<String>(); paid_search.add("Google.com"); paid_search.add("MSN.com"); paid_search.add("Yahoo! Inc");
		
		digital_video  = new ArrayList<String>(); digital_video.add("ABCNEWS.com"); digital_video.add("CNN.com"); digital_video.add("Google.com");digital_video.add("CNNMoney");
		digital_video.add("HLN");digital_video.add("Tennis Channel.com");
		
		acquisition = new ArrayList<String>(); acquisition.add("CPX"); acquisition.add("America Online (AOL)"); acquisition.add("NM Evolution"); acquisition.add("Value Click");
		
		Affiliate_Marketing = new ArrayList<String>();Affiliate_Marketing.add("BestCashCow.com"); Affiliate_Marketing.add("Commission Junction"); 
		Affiliate_Marketing.add("Depositaccounts.com"); Affiliate_Marketing.add("Google.com"); Affiliate_Marketing.add("gobankingrates.com"); Affiliate_Marketing.add("Money Rates");
		
		behavioral_Targeting = new ArrayList<String>(); behavioral_Targeting.add("Google.com");behavioral_Targeting.add("About.com");behavioral_Targeting.add("Forbes.com");behavioral_Targeting.add("Fortune.com");
		behavioral_Targeting.add("CNNMoney");behavioral_Targeting.add("Accordant Media");behavioral_Targeting.add("ADBLADE");behavioral_Targeting.add("Alloy");
		behavioral_Targeting.add("Adconion Media Group");behavioral_Targeting.add("America Online (AOL)");behavioral_Targeting.add("BrightRoll");behavioral_Targeting.add("DBG");
		behavioral_Targeting.add("Crisp Media");behavioral_Targeting.add("Demand Media");behavioral_Targeting.add("edufundingpartners.com");behavioral_Targeting.add("Genome");
		behavioral_Targeting.add("Kiplinger's");behavioral_Targeting.add("placeiq.com");behavioral_Targeting.add("edufundingpartners.com");behavioral_Targeting.add("Genome");
		behavioral_Targeting.add("MSN.com");behavioral_Targeting.add("RadiumOne");behavioral_Targeting.add("Rhymth");behavioral_Targeting.add("Sales Spider");
		behavioral_Targeting.add("Scripps Network");behavioral_Targeting.add("Synacor");behavioral_Targeting.add("Value Click");behavioral_Targeting.add("VivaKi AOD");
		behavioral_Targeting.add("SpotXange");behavioral_Targeting.add("TubeMogul");behavioral_Targeting.add("Videology");behavioral_Targeting.add("X plus 1"); behavioral_Targeting.add("Yahoo! Inc");
		
		co_branded = new ArrayList<String>(); co_branded.add("CNNMoney");co_branded.add("ABCNEWS.com");co_branded.add("NPR Online");

		adNetwork = new ArrayList<String>();adNetwork.add("AT&T Network"); adNetwork.add("MdotM"); adNetwork.add("Millennial Media"); adNetwork.add("Mobile Theory"); 
		adNetwork.add("Crisp Media"); adNetwork.add("HipCricket"); adNetwork.add("Martini media network"); adNetwork.add("Mojiva"); adNetwork.add("NPR Online");
		adNetwork.add("Value Click"); adNetwork.add("VivaKi AOD"); adNetwork.add("Yahoo! Inc"); adNetwork.add("RocketFuel, Inc");

		retargeting  = new ArrayList<String>();	retargeting.add("ADBLADE"); retargeting.add("Yahoo! Inc"); retargeting.add("X plus 1"); retargeting.add("VivaKi AOD"); retargeting.add("RocketFuel, Inc");
		retargeting.add("America Online (AOL)"); retargeting.add("QuantCast"); retargeting.add("OwnerIQ"); retargeting.add("MSN.com"); retargeting.add("DataXU"); retargeting.add("Google.com");
		retargeting.add("Aod - Invite"); retargeting.add("Investingchannel.com"); retargeting.add("Collective Media"); retargeting.add("Dstillery"); retargeting.add("Genome");
		
		front_page  = new ArrayList<String>();front_page.add("CNNMoney"); front_page.add("WSJ.com"); front_page.add("Washingtonpost.com"); front_page.add("Value Click"); front_page.add("USA Today");
		front_page.add("ABCNEWS.com"); front_page.add("Time Inc Network"); front_page.add("TheStreet.com"); front_page.add("Synacor"); front_page.add("Reuters.com");
		front_page.add("Amazon.com"); front_page.add("NBC Universal - NBC Entertainment"); front_page.add("MSN.com"); front_page.add("Investopedia.com"); front_page.add("BBC");
		front_page.add("America Online (AOL)");front_page.add("America Online (AOL)");front_page.add("America Online (AOL)");front_page.add("America Online (AOL)");front_page.add("America Online (AOL)");
		front_page.add("AOL - The Huffington Post"); front_page.add("Atlantic Monthly"); front_page.add("Bloomberg.com"); front_page.add("CNBC.com"); front_page.add("CNN.com");
		front_page.add("foxnews.com");front_page.add("foxnews.com");front_page.add("foxnews.com");front_page.add("foxnews.com");front_page.add("foxnews.com");front_page.add("Google.com");
		
		bankrate  = new ArrayList<String>(); bankrate.add("Bankrate");
		
		email  = new ArrayList<String>(); email.add("mybank.com");
		
		enhanced_Targeting  = new ArrayList<String>(); enhanced_Targeting.add("About.com");enhanced_Targeting.add("choicestream.com");enhanced_Targeting.add("MSN.com");
		enhanced_Targeting.add("Accordant Media");enhanced_Targeting.add("America Online (AOL)");enhanced_Targeting.add("Aod - Invite");enhanced_Targeting.add("Bizo");enhanced_Targeting.add("Yahoo! Inc");
		enhanced_Targeting.add("AOD-VIDEO");enhanced_Targeting.add("Dstillery");enhanced_Targeting.add("Grab Media");enhanced_Targeting.add("NetShelter");enhanced_Targeting.add("NM Evolution");
		enhanced_Targeting.add("BrightRoll");enhanced_Targeting.add("DataXU");enhanced_Targeting.add("Google.com");enhanced_Targeting.add("Media6Degrees");enhanced_Targeting.add("OwnerIQ");
		enhanced_Targeting.add("Casale");enhanced_Targeting.add("CPX");enhanced_Targeting.add("Genome");enhanced_Targeting.add("MaxPoint Interactive");enhanced_Targeting.add("PulsePoint");
		enhanced_Targeting.add("QuantCast");enhanced_Targeting.add("Smartclip");enhanced_Targeting.add("SpotXange");enhanced_Targeting.add("Turner Media Group");enhanced_Targeting.add("Ziff Davis");
		enhanced_Targeting.add("RadiumOne");enhanced_Targeting.add("Shift.com");enhanced_Targeting.add("Triad Digital");enhanced_Targeting.add("Time Inc Network");enhanced_Targeting.add("YuMe");
		enhanced_Targeting.add("RocketFuel, Inc");enhanced_Targeting.add("Undertone Network");enhanced_Targeting.add("Value Click");enhanced_Targeting.add("VivaKi AOD");enhanced_Targeting.add("X plus 1");
		
		
		finance  = new ArrayList<String>(); finance.add("WSJ.com");finance.add("Marchex");finance.add("MSN.com");finance.add("IBTimes.com");finance.add("findthebest.com");finance.add("ABCNEWS.com");
		finance.add("Martini media network");finance.add("Live Intent, Inc");finance.add("Mint.com");finance.add("Google.com");finance.add("Depositaccounts.com");finance.add("About.com");
		finance.add("Yahoo! Inc");finance.add("LearnVest");finance.add("InvestorGuide.com");finance.add("gobankingrates.com");finance.add("Demand Media");finance.add("BestCashCow.com");
		finance.add("Moneyaisle");finance.add("Kiplinger's");finance.add("Investopedia.com");finance.add("Finser Media");finance.add("Credit Karma");finance.add("Bloomberg.com");
		finance.add("Mediafed");finance.add("Investing Media Solutions");finance.add("Investingchannel.com");finance.add("Business Week");finance.add("BrightRoll");finance.add("Forbes.com");
		finance.add("CNNMoney");finance.add("mybank.com");finance.add("Genome");finance.add("America Online (AOL)");finance.add("Fortune.com");finance.add("CNN.com");
		finance.add("MyBankTracker");finance.add("Wall St. Cheat Sheet");finance.add("Tremor");finance.add("TheStreet.com");finance.add("QuinStreet");
		finance.add("NASDAQ");finance.add("VivaKi AOD");finance.add("Time Inc Network");finance.add("Sales Spider");finance.add("NM Evolution");
		
		organic_search  = new ArrayList<String>(); organic_search.add("Google.com");
		
		rate_tables = new ArrayList<String>(); rate_tables.add("Depositaccounts.com"); rate_tables.add("gobankingrates.com");rate_tables.add("Google.com");rate_tables.add("findthebest.com");
		rate_tables.add("Bankrate");rate_tables.add("BestCashCow.com");rate_tables.add("Informa");rate_tables.add("QuinStreet");rate_tables.add("Mint.com");rate_tables.add("Money Rates");rate_tables.add("MyBankTracker");
		
		news  = new ArrayList<String>(); news.add("Forbes.com");news.add("DBG");news.add("CNBC.com");news.add("Bizjournals");news.add("CBS.com");news.add("America Online (AOL)");
		news.add("foxnews.com");news.add("CNNMoney");news.add("CNN.com");news.add("Centro");news.add("APWA.com");news.add("BBC");news.add("ABCNEWS.com");news.add("Atlantic Monthly");
		news.add("MSN.com");news.add("Crisp Media");news.add("newsweek.com");news.add("HLN");news.add("Reuters.com");news.add("National Geographic");news.add("SpotXange");news.add("US News");
		news.add("QuadrantONE");news.add("NPR Online");news.add("New Yorker");news.add("New York Times");news.add("RocketFuel, Inc");news.add("Smithsonian Magazine");news.add("VivaKi AOD");news.add("USA Today");
		news.add(" Week Magazine");news.add("Yahoo! Inc");news.add("WSJ.com");news.add("Tremor");news.add("Synacor");news.add("Time Inc Network");news.add("Washingtonpost.com");
		 
		mobile  = new ArrayList<String>(); mobile.add("CNN.com");mobile.add("CNNMoney");mobile.add("Google");mobile.add("MSN.com");mobile.add("VivaKi AOD");mobile.add("Network Push");mobile.add("NPR Online");
		
		Ipad  = new ArrayList<String>(); Ipad.add("MSN.com");Ipad.add("Google.com");
		
		portal_homepage  = new ArrayList<String>(); portal_homepage.add("MSN.com");
		
		financeNoImp  = new ArrayList<String>(); financeNoImp.add("Depositaccounts.com");financeNoImp.add("gobankingrates.com");financeNoImp.add("Mint.com");

		retirement = new ArrayList<String>(); retirement.add("Demand Media");retirement.add("MSN.com");retirement.add("About.com");retirement.add("Yahoo! Inc");
		
		social_retargeting = new ArrayList<String>();social_retargeting.add("DataXU");
		
		sports = new ArrayList<String>(); sports.add("gobankingrates.com");sports.add("Bankrate");sports.add("Tennis Channel.com");sports.add("CNN.com");
		
		lifestyle  = new ArrayList<String>(); lifestyle.add("Forbes.com");lifestyle.add("Hooklogic");lifestyle.add("DBG");lifestyle.add("ABCNEWS.com");lifestyle.add("Tribal Fusion Ad Network");
		lifestyle.add("Scripps Network");lifestyle.add("move.realtor.com");lifestyle.add("Monster.com");lifestyle.add("Demand Media");lifestyle.add("IVdopia");lifestyle.add("CNBC.com");
		lifestyle.add("Slate");lifestyle.add("MSN.com");lifestyle.add("Men's Journal.com");lifestyle.add("Expedia");lifestyle.add("BonAppetit.com");lifestyle.add("CBS.com");
		lifestyle.add("Synacor");lifestyle.add("nccmedia.com");lifestyle.add("Glassdoor, Inc");lifestyle.add("Genome");lifestyle.add("America Online (AOL)");lifestyle.add("Alloy");
		lifestyle.add("Triad Digital");lifestyle.add("NetShelter");lifestyle.add("Fast Company");lifestyle.add("La Cucina");lifestyle.add("Amazon.com");lifestyle.add("Adara");
		lifestyle.add("Tremor");lifestyle.add("PNDRMDOX2512");lifestyle.add("Conde Nast Publications");lifestyle.add("LinkedIn");lifestyle.add("140 Proof");lifestyle.add("AARP");
		lifestyle.add("hotwire.com");lifestyle.add("hulu.com");lifestyle.add("Golf.com");lifestyle.add("Forbes.com");lifestyle.add("Entrepreneur.com");lifestyle.add("Travelocity.com");
		lifestyle.add("Trulia");lifestyle.add("WeatherBug");lifestyle.add("Videology");lifestyle.add("Urbanspoon");lifestyle.add("TubeMogul");
		lifestyle.add("Zillow");lifestyle.add("Zagat");lifestyle.add("VivaKi AOD");lifestyle.add("Weather.com");lifestyle.add("Yahoo! Inc");lifestyle.add("Ziff Davis");
		
 	}
 	
 	
	
	public Records()
	{
		this.records = new ArrayList<Record>();
		this.awareness = new Strategy("Awareness");
		this.engagement = new Strategy("Engagement");
		this.conversion = new Strategy("Conversion");
		consistency_matrix = new ArrayList<ArrayList<String>>();
		// adding awareness
		for(int i=0;i<awareness.ccg.size();i++)
		{
			for(int j=0;j<awareness.ccg.get(i).CC.size();j++)
			{
				ArrayList<String> row = new ArrayList<String>();
				row.add(awareness.ccg.get(i).CC.get(j));
				row.add(awareness.ccg.get(i).name);
				row.add(awareness.name);
				consistency_matrix.add(row);
			}	
		}
		// adding engagement
		for(int i=0;i<engagement.ccg.size();i++)
		{
			
					
			for(int j=0;j<engagement.ccg.get(i).CC.size();j++)
			{
				ArrayList<String> row = new ArrayList<String>();
				row.add(engagement.ccg.get(i).CC.get(j));
				row.add(engagement.ccg.get(i).name);
				row.add(engagement.name);
				consistency_matrix.add(row);
			}
			
		}
		// adding conversion
		for(int i=0;i<conversion.ccg.size();i++)
		{
			
							
			for(int j=0;j<conversion.ccg.get(i).CC.size();j++)
			{
				ArrayList<String> row = new ArrayList<String>();
				row.add(conversion.ccg.get(i).CC.get(j));
				row.add(conversion.ccg.get(i).name);
				row.add(conversion.name);
				consistency_matrix.add(row);
			}
			
		}
		fill_fields();
	}
	
	@SuppressWarnings("finally")
	public boolean create_population()
	{
		boolean success = false;
		
		String query = "SELECT ";
		
		String static_query = "SELECT [ActivityDate],[CampaignName],[DFA_Site_Name],[Placement_Group_Strategy],[SiteContentCategory],"+
		"[ContentCatGroup],[Product],[App_Status],[ProductType],[Rate_Type],[DFASumImps],[DFASumClicks],[DFA_Accounts],"+
		"[DFARevenue],[DFA_Revenue],[Attribution],[ActivityRevenue_1],[EV_1],[EstimatedBalance_1],[MediaCost],[DFA_MediaCost],"+
		"[Site_Name],[MobileDevice],[DeviceViewType],[DeviceOS],[TotalProduct],[ID], [SitePlacementName] FROM [SMGPMDemo].[dbo].[RPT_SPBPOD]";
				
		for(int i=0;i<fields.size();i++)
		{
			query += fields.get(i);
			if (i!= (fields.size()-1)) query +=", ";
			//else query +=" ";
		}
		
		query +=" FROM " + tableName;
		try 
        {
            connection = ConnectionFactory.getConnection();
            statement = connection.createStatement();
            resultSet =statement.executeQuery(static_query);
            
            
            Record temp ;
            System.out.print(">>Query to retrieve data being executed... ");
            //if (fields.size() != records.get(0).getColumnCount()) System.out.print("ERROR: number of fields mismatches the number of records attributes... ");
            while(resultSet .next())
            {
            	temp = new Record();
            	if (resultSet.getObject("ID") != null) temp.ID = new Integer(resultSet.getInt("ID"));
            	else temp.ID = -1;
            	if (resultSet.getDate("ActivityDate") != null) temp.ActivityDate = resultSet.getDate("ActivityDate");
            	else temp.ActivityDate = new Date();
            	if (resultSet.getString("CampaignName") != null) temp.CampaignName = new String(resultSet.getString("CampaignName"));
            	else temp.CampaignName = new String(NULL);
            	if (resultSet.getString("ContentCatGroup") != null) temp.ContentCatGroup = new String(resultSet.getString("ContentCatGroup"));
            	else temp.ContentCatGroup = new String(NULL);
            	if (resultSet.getString("DFA_Site_Name") != null) temp.DFA_Site_Name = new String(resultSet.getString("DFA_Site_Name"));
            	else temp.DFA_Site_Name = new String(NULL);
            	if (resultSet.getObject("MediaCost") != null) temp.MediaCost = new Double(resultSet.getDouble("MediaCost"));
            	else temp.MediaCost = 0.0;
            	if (resultSet.getString("Placement_Group_Strategy") != null) temp.Placement_Group_Strategy = new String(resultSet.getString("Placement_Group_Strategy"));
            	else temp.Placement_Group_Strategy = new String(NULL);
            	if (resultSet.getString("Product") != null) temp.Product = new String(resultSet.getString("Product"));
            	else temp.Product = new String(NULL);
            	if (resultSet.getString("ProductType") != null) temp.ProductType = new String(resultSet.getString("ProductType"));
            	else temp.ProductType = new String(NULL);
            	if (resultSet.getObject("EV_1") != null) temp.Profitability = new Double(resultSet.getDouble("EV_1"));
            	else temp.Profitability = 0.0;
            	if (resultSet.getObject("Attribution") != null) temp.Attribution = new Double(resultSet.getDouble("Attribution"));
            	else temp.Attribution = 0.0;
            	if (resultSet.getObject("EstimatedBalance_1") != null) temp.EstimatedBalance_1 = new Double(resultSet.getDouble("EstimatedBalance_1"));
            	else temp.EstimatedBalance_1 = 0.0;
            	if (resultSet.getObject("DFA_Accounts") != null) temp.DFA_Accounts = new Double(resultSet.getDouble("DFA_Accounts"));
            	else temp.DFA_Accounts = 0.0;
            	if (resultSet.getObject("ActivityRevenue_1") != null) temp.ActivityRevenue_1 = new Double(resultSet.getDouble("ActivityRevenue_1"));
            	else temp.ActivityRevenue_1 = 0.0;
            	if (resultSet.getObject("DFA_Revenue") != null) temp.DFA_Revenue = new Double(resultSet.getDouble("DFA_Revenue"));
            	else temp.DFA_Revenue = 0.0;
            	if (resultSet.getObject("DFASumImps") != null) temp.DFASumImps = new Double(resultSet.getDouble("DFASumImps"));
            	else temp.DFASumImps = 0.0;
            	if (resultSet.getObject("DFASumClicks") != null) temp.DFASumClicks = new Double(resultSet.getDouble("DFASumClicks"));
            	else temp.DFASumClicks = 0.0;
            	if (resultSet.getString("SiteContentCategory") != null) temp.SiteContentCategory = new String(resultSet.getString("SiteContentCategory"));
            	else temp.SiteContentCategory = new String(NULL);
            	if (resultSet.getString("MobileDevice") != null) temp.MobileDevice = new String(resultSet.getString("MobileDevice"));
            	else temp.MobileDevice = new String(NULL);
            	if (resultSet.getString("DeviceViewType") != null) temp.DeviceViewType = new String(resultSet.getString("DeviceViewType"));
            	else temp.DeviceViewType = new String(NULL);
            	if (resultSet.getString("DeviceOS") != null) temp.DeviceOS = new String(resultSet.getString("DeviceOS"));
            	else temp.DeviceOS = new String(NULL);
            	if (resultSet.getString("TotalProduct") != null) temp.TotalProduct = new String(resultSet.getString("TotalProduct"));
            	else temp.TotalProduct = new String(NULL);
            	if (resultSet.getString("SitePlacementName") != null) temp.SitePlacementName = new String(resultSet.getString("SitePlacementName"));
            	else temp.SitePlacementName = new String(NULL);
            	if (resultSet.getString("Site_Name") != null) temp.Site_Name = new String(resultSet.getString("Site_Name"));
            	else temp.Site_Name = new String(NULL);
            	records.add(temp);
            	
            	
            }
            if (records.size()!= 0) 
            	{
            		success = true;
            		System.out.print("Successfully queried "+records.size()+" records \n");
            	}
            SQLWarning warning = statement.getWarnings();
            if (warning != null)
            {
            	warning.printStackTrace();
            	throw new ApplicationException(warning.getMessage());
            }
            
                
        } 
        catch (SQLException e) 
        {
            Tools.printSQLException(e);
        	ApplicationException exception = new ApplicationException(e.getMessage(), e);
            exception.printStackTrace();
            throw exception;
        } 
        finally 
        {
            Tools.close(statement);
            Tools.close(connection);
            Tools.close(resultSet);
            //System.out.println("Query Successfully Executed, successfully got "+queried_values.size()+" of column "+columnName);
            this.sort_by_date();
            return success;
        }
	}
	
	
	
	public void update_values(String field, Double min_range, Double max_range, Double min_random_factor, Double max_random_factor)
	{
		// sort data first from earliest to latest date
		this.sort_by_date();
		ArrayList<Double> results = new ArrayList<Double>();
		int size = records.size();
		results = Tools.progressive_values_generator(size, min_range, max_range, min_random_factor, max_random_factor);
		
		for(int i=0;i<records.size();i++)
		{
			if (field.equals("Profitability") || field.equals("EV_1")) records.get(i).Profitability = results.get(i);
			else if (field.equals("MediaCost")) records.get(i).MediaCost = results.get(i);
		}
		
	}
	
	public void info ()
	{
		
		int number_invalid_IDs = 0;
		int number_invalid_strategy = 0;
		int number_invalid_ContentCatGroup = 0;
		int number_invalid_Product = 0;
		int number_invalid_ProductType = 0;
		int number_invalid_TotalProduct = 0;
		int number_invalid_Ipad = 0 ;
		int number_valid_Ipad = 0 ;
		
		for(int i=0;i<records.size();i++)
		{
			if (records.get(i).ID == -1) number_invalid_IDs++;
			if (records.get(i).Placement_Group_Strategy.equals(NULL)) number_invalid_strategy++;
			if (records.get(i).ContentCatGroup.equals(NULL)) number_invalid_ContentCatGroup++;
			if (records.get(i).Product.equals(NULL)) number_invalid_Product++;
			if (records.get(i).ProductType.equals(NULL)) number_invalid_ProductType++;
			if (records.get(i).TotalProduct.equals(NULL)) number_invalid_TotalProduct++;
			if (records.get(i).SiteContentCategory.equals("IPad")) {number_invalid_Ipad++;}
			if (records.get(i).SiteContentCategory.equals("Ipad")) {number_valid_Ipad++;}
		}
		System.out.println("\n>number of invalid IDs: "+number_invalid_IDs);
		System.out.println(">number of invalid Placement_Group_Strategys: "+number_invalid_strategy);
		System.out.println(">number of invalid ContentCatGroup: "+number_invalid_ContentCatGroup);
		System.out.println(">number of invalid Product: "+number_invalid_Product);
		System.out.println(">number of invalid ProductType "+number_invalid_ProductType);
		System.out.println(">number of invalid TotalProduct "+number_invalid_TotalProduct);
		System.out.println(">number of invalid IPAD "+number_invalid_Ipad+" / "+number_valid_Ipad);
		
		
		Double profitability_median = 0.0 ;
		Double mediaCost_median = 0.0 ;
		Double medianEstimatedBalance_1 = 0.0;
		Double medianAttribution = 0.0 ;
		Double medianDFA_accounts = 0.0 ;
		Double medianClicks = 0.0 ;
		Double medianImpressions = 0.0 ;
		Double median_ROAS = 0.0;
		Double median_ProductType_OnlineSavings = 0.0 ;
		Double median_Retargeting_pf = 0.0 , median_Retargeting_dfa = 0.0, median_RT_pf = 0.0 , median_RT_dfa = 0.0 ;
		int counter_Retargeting = 0 , counter_RT = 0 ;
		
		for(int i=0;i<records.size();i++)
		{
			profitability_median += records.get(i).Profitability ;
			mediaCost_median += records.get(i).MediaCost ;
			medianEstimatedBalance_1 += records.get(i).EstimatedBalance_1 ;
			medianAttribution += records.get(i).Attribution ;
			medianDFA_accounts += records.get(i).DFA_Accounts ;
			medianClicks += records.get(i).DFASumClicks ;
			medianImpressions += records.get(i).DFASumImps ;
			if (records.get(i).ProductType.equals("Online Savings")){median_ProductType_OnlineSavings++;}
			if (records.get(i).SiteContentCategory.equals("Retargeting")) {median_Retargeting_pf += records.get(i).Attribution ;}
			if (records.get(i).SiteContentCategory.equals("Retargeting")) {median_Retargeting_dfa += records.get(i).DFA_Accounts ; counter_Retargeting++;}
			if (records.get(i).SiteContentCategory.equals("Rate Tables")) {median_RT_pf += records.get(i).Attribution ; }
			if (records.get(i).SiteContentCategory.equals("Rate Tables")) {median_RT_dfa += records.get(i).DFA_Accounts ; counter_RT++;}
			
		}
		
		profitability_median /= records.size();
		mediaCost_median /= records.size();
		medianEstimatedBalance_1 /= records.size();
		medianAttribution /= records.size();
		medianDFA_accounts /= records.size();
		medianClicks /= records.size();
		medianImpressions /= records.size();
		median_ProductType_OnlineSavings /= records.size();
		median_Retargeting_pf /= counter_Retargeting ;
		median_Retargeting_dfa /= counter_Retargeting ;
		median_RT_pf /= counter_RT ;
		median_RT_dfa /= counter_RT ;
		median_ROAS = profitability_median / mediaCost_median ;
		
		System.out.println("\n>Median of Profitability: "+profitability_median);
		System.out.println(">Median of MediaCost: "+mediaCost_median);
		System.out.println(">Median of EstimatedBalance_1: "+medianEstimatedBalance_1);
		System.out.println(">Median of Attribution: "+medianAttribution);
		System.out.println(">Median of DFA_Accounts: "+medianDFA_accounts);
		System.out.println(">Median of medianClicks: "+medianClicks);
		System.out.println(">Median of medianImpressions: "+medianImpressions);
		System.out.println(">Median of ROAS: "+median_ROAS+"\n");
		System.out.println(">Median of Online Savings: "+median_ProductType_OnlineSavings+"\n");
		
		System.out.println(">Median of Retargeting: "+get_median_contentCategory("Retargeting")+"\n");
		System.out.println(">Median of Paid Search: "+get_median_contentCategory("Paid Search")+"\n");
		System.out.println(">Median of Social Retargeting: "+get_median_contentCategory("Social Retargeting")+"\n");
		System.out.println(">Median of Finance: "+get_median_contentCategory("Finance")+"\n");
		System.out.println(">Median of Paid BankrateFin: "+get_median_contentCategory("BankrateFin")+"\n");
		System.out.println(">Median of Paid Search: "+get_median_contentCategory("Rate Tables")+"\n");
		
		System.out.println(">Median of Retargeting PF accounts: "+median_Retargeting_pf+"\n");
		System.out.println(">Median of Retargeting DFA accounts: "+median_Retargeting_dfa+"\n");
		System.out.println(">Median of RT PF accounts: "+median_RT_pf+"\n");
		System.out.println(">Median of RT DFA accounts: "+median_RT_dfa+"\n");
	}
	
	public double get_median_contentCategory(String contentCategory)
	{
		double profitability_median = 0.0;
		double mediaCost_median = 0.0;
		int counter=0;
		for(int i=0;i<records.size();i++)
		{
			if (records.get(i).SiteContentCategory.equals(contentCategory)) 
			{
				profitability_median += records.get(i).Profitability ;
				mediaCost_median += records.get(i).MediaCost ;
				counter++;
			}
		}
		profitability_median /= counter;
		mediaCost_median /= counter;
		return profitability_median / mediaCost_median ;
	}
	
	public int print_records_number_in_period(DateTime start, DateTime end)
	{
		int counter =0;
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) counter++;;
		}
		System.out.print("\n> "+counter+" elements between "+start.toString()+" and "+end.toString());
		
		return counter;
	}
	
	public void sort_by_date()
	{
		//this.print_population(10);
		Collections.sort(records, new Comparator<Record>() 
				{
					   public int compare(Record o1, Record o2) 
					   {
					      Date a = o1.ActivityDate;
					      Date b = o2.ActivityDate;
					      
					        return o1.ActivityDate.compareTo(o2.ActivityDate);
					   }
				});
		//this.print_population(10);
	}
	
	public void set_array_values (String field, Double min_target, Double max_target, Double min_random_factor, Double max_random_factor)
	{
		ArrayList<Double> temp = new ArrayList<Double>();
		temp = Tools.progressive_values_generator(this.records.size(), min_target, max_target, min_random_factor, max_random_factor);
        System.out.println(">Median value of generated array: "+Tools.median_value(temp));
        //this.sort_by_date();
        
        for(int i=0;i<records.size();i++)
		{ 
        	if (field.equals("Attribution")) records.get(i).Attribution = temp.get(i);
        	else if (field.equals("EstimatedBalance_1")) records.get(i).EstimatedBalance_1 = temp.get(i);
        	else if (field.equals("MediaCost")) records.get(i).MediaCost = temp.get(i);
        	else if (field.equals("Profitability")) records.get(i).Profitability = temp.get(i); 
		}
	}
	
	public void set_array_values_on_period (String field, Double min_target, Double max_target, Double min_random_factor, Double max_random_factor, DateTime start, DateTime end)
	{
		ArrayList<Double> temp = new ArrayList<Double>();
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) indexes.add(i);
		}
		
		temp = Tools.progressive_values_generator(indexes.size(), min_target, max_target, min_random_factor, max_random_factor);
        //System.out.println(">changing values of "+indexes.size()+" rows of "+field);
        
        for(int i=0;i<indexes.size();i++)
		{ 
        		if (field.equals("Attribution")) records.get(indexes.get(i)).Attribution = temp.get(i);
        		else if (field.equals("EstimatedBalance_1")) records.get(indexes.get(i)).EstimatedBalance_1 = temp.get(i);
        		else if (field.equals("MediaCost")) records.get(indexes.get(i)).MediaCost = temp.get(i);
        		else if (field.equals("Profitability")) records.get(indexes.get(i)).Profitability = temp.get(i);
        		else if (field.equals("DFA_Accounts")) records.get(indexes.get(i)).DFA_Accounts = temp.get(i);
        		else if (field.equals("DFASumImps")) records.get(indexes.get(i)).DFASumImps = temp.get(i);
        		else if (field.equals("DFASumClicks")) records.get(indexes.get(i)).DFASumClicks = temp.get(i);
        		else if (field.equals("ActivityRevenue_1")) records.get(indexes.get(i)).ActivityRevenue_1 = temp.get(i);
		}
	}
	
	
	@SuppressWarnings("finally")
	public boolean update_database()
	{
		boolean success = false;
		PreparedStatement update = null;
		try 
        {
            connection = ConnectionFactory.getConnection();
            
            String updateQuery = "UPDATE "+tableName+" SET "+
            "ActivityDate = ?, ContentCatGroup = ?, DFA_Site_Name = ?, MediaCost = ?, Placement_Group_Strategy = ?, "+
            "Product = ?, ProductType = ?, EV_1 = ?, SiteContentCategory = ?, Attribution = ?, EstimatedBalance_1 = ?, "+
            "DFA_Accounts = ?, ActivityRevenue_1 = ?, DFASumImps = ?, DFASumClicks = ?, TotalProduct = ?, Site_Name = ?, DFA_Revenue = ?,"+
            "MobileDevice = ?, DeviceViewType = ?, DeviceOS = ?, SitePlacementName = ?, CampaignName = ?"+
            " WHERE ID = ?";
            
            update = connection.prepareStatement(updateQuery);
            connection.setAutoCommit(false);
            System.out.println("\n>Updating the database... ");
            ProgressBar bar = new ProgressBar();
            int old_value = -1;
            for(int i=0;i<records.size();i++)
    		{
            	update.setDate   (1, (java.sql.Date) records.get(i).ActivityDate);
                update.setString (2, records.get(i).ContentCatGroup);
                update.setString (3, records.get(i).DFA_Site_Name);
                update.setDouble (4, records.get(i).MediaCost);
                update.setString (5, records.get(i).Placement_Group_Strategy);
                update.setString (6, records.get(i).Product);
                update.setString (7, records.get(i).ProductType);
                update.setDouble (8, records.get(i).Profitability);
                update.setString (9, records.get(i).SiteContentCategory);
                update.setDouble(10, records.get(i).Attribution);
                update.setDouble(11, records.get(i).EstimatedBalance_1);
                update.setDouble(12, records.get(i).DFA_Accounts);
                update.setDouble(13, records.get(i).ActivityRevenue_1);
                update.setDouble(14, records.get(i).DFASumImps);
                update.setDouble(15, records.get(i).DFASumClicks);
                update.setString(16, records.get(i).TotalProduct);
                update.setString(17, records.get(i).Site_Name);
                update.setDouble(18, records.get(i).DFA_Revenue);
                update.setString(19, records.get(i).MobileDevice);
                update.setString(20, records.get(i).DeviceViewType);
                update.setString(21, records.get(i).DeviceOS);
                update.setString(22, records.get(i).SitePlacementName);
                update.setString(23, records.get(i).CampaignName);
                update.setInt   (24, records.get(i).ID);
                update.executeUpdate();
                connection.commit();
                //System.out.print(records.get(i).ID+" ");
                //if (i % 10 == 0) System.out.print("\n");
                old_value = bar.update(i, old_value, records.size());
    		}
            connection.setAutoCommit(true);
            
            success = true;
            if (success == true) 
            	{
            		System.out.println("\n>Successfully updated "+records.size()+" records \n");
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
            return success;
        }
	}
	
	public void print_population(int print_size)
	{
		System.out.println("***--- Printing "+print_size+" of "+this.records.size()+" records ---***\n");
		for(int i=0;i<print_size;i++)
		{
			records.get(i).print_record();
		}
	}
	
	public void resolve_ccg_strategy_inconsistency()
	{
		int counter = 0, idx = 0 ;		
		for(int i=0;i<records.size();i++)
		{
			for(int j=0;j<UserPreferences.content_category.size();j++)
			{
				if (records.get(i).SiteContentCategory.equals(UserPreferences.content_category.get(j)))
				{
					idx = new Random().nextInt(UserPreferences.cc_to_ccg.get(j).size());
					records.get(i).ContentCatGroup = new String(UserPreferences.cc_to_ccg.get(j).get(idx));
					for(int k=0;k<UserPreferences.content_category_group.size();k++)
					{
						if (records.get(i).ContentCatGroup.equals(UserPreferences.content_category_group.get(k)))
						{
							idx = new Random().nextInt(UserPreferences.ccg_to_strategy.get(k).size());
							records.get(i).Placement_Group_Strategy = new String(UserPreferences.ccg_to_strategy.get(k).get(idx));
							counter++;
						}
					}
				}
			}
		}
		for(int i=0;i<records.size();i++)
		{
			if (records.get(i).SiteContentCategory.equals("IPad")) records.get(i).SiteContentCategory = new String("Ipad");
		}
		System.out.println(">Number of resolved inconsistencies: "+counter);
	}
	
	public void resolve_compaingn_name_inconsistency()
	{
		int counter = 0, idx = 0 ;
		String name = new String();
		for(int i=0;i<records.size();i++)
		{
			name = new String();
			name += new DateTime(records.get(i).ActivityDate).getYear();
			name += "-MyBank-";
			for(int j=0;j<UserPreferences.ccg_abbreviation.size();j++)
			{
				if (records.get(i).ContentCatGroup.equals(UserPreferences.content_category_group.get(j)))
				{
					name +=  UserPreferences.ccg_abbreviation.get(j);
				}
			}
			name += "-";
			
			if(records.get(i).Placement_Group_Strategy.equals("Awareness")) name += "AWR";
			else if(records.get(i).Placement_Group_Strategy.equals("Conversion")) name += "CON";
			else if(records.get(i).Placement_Group_Strategy.equals("Engagement")) name += "ENG";
			
			records.get(i).CampaignName = new String(name);
		}
		System.out.println(">Number of renamed compaigns: "+counter);
	}
	
	public void resolve_sites_inconsistency()
	{
		int counter = 0 ;		
		counter += Tools.update_sites_per_cc("Paid Search", paid_search, records);
		counter += Tools.update_sites_per_cc("Retargeting", retargeting, records);
		counter += Tools.update_sites_per_cc("Frontpage", front_page, records);
		counter += Tools.update_sites_per_cc("BankrateFin", bankrate, records);
		counter += Tools.update_sites_per_cc("BankrateSpecial", bankrate, records);
		counter += Tools.update_sites_per_cc("Acquisition", acquisition, records);
		counter += Tools.update_sites_per_cc("Ad Network", adNetwork, records);
		counter += Tools.update_sites_per_cc("Affiliate Marketing", Affiliate_Marketing, records);
		counter += Tools.update_sites_per_cc("Behavioral Targeting", behavioral_Targeting, records);
		counter += Tools.update_sites_per_cc("Co-branded", co_branded, records);
		counter += Tools.update_sites_per_cc("Email", email, records);
		counter += Tools.update_sites_per_cc("Enhanced Targeting", enhanced_Targeting, records);
		counter += Tools.update_sites_per_cc("Finance", finance, records);
		counter += Tools.update_sites_per_cc("Digital Video", digital_video, records);
		
		counter += Tools.update_sites_per_cc("Organic Search", organic_search, records);
		counter += Tools.update_sites_per_cc("Rate Tables", rate_tables, records);
		counter += Tools.update_sites_per_cc("News", news, records);
		counter += Tools.update_sites_per_cc("Mobile", mobile, records);
		counter += Tools.update_sites_per_cc("Ipad", Ipad, records);
		counter += Tools.update_sites_per_cc("IPad", Ipad, records);
		counter += Tools.update_sites_per_cc("Lifestyle", lifestyle, records);
		counter += Tools.update_sites_per_cc("FinanceNoImp", financeNoImp, records);
		
		counter += Tools.update_sites_per_cc("Retirement", retirement, records);
		counter += Tools.update_sites_per_cc("Portal Homepage", portal_homepage, records);
		counter += Tools.update_sites_per_cc("Social Retargeting", social_retargeting, records);
		counter += Tools.update_sites_per_cc("Sports", sports, records);
		
		System.out.println(">Number of resolved site inconsistencies: "+counter);
	}
	
	public int resolve_placements_inconsistency()
    {
    	int idx ;
    	int counter = 0 ;
    	String temp, name ;
    	
    	for(int i=0;i<records.size();i++)
		{
    		name = new String();
			name += records.get(i).Site_Name;
			name += "_";
			for(int j=0;j<UserPreferences.ccg_abbreviation.size();j++)
			{
				if (records.get(i).ContentCatGroup.equals(UserPreferences.content_category_group.get(j)))
				{
					name +=  UserPreferences.ccg_abbreviation.get(j);
				}
			}
			name += "_";
			for(int j=0;j<UserPreferences.cc_abbreviation.size();j++)
			{
				if (records.get(i).SiteContentCategory.equals(UserPreferences.content_category.get(j)))
				{
					name +=  UserPreferences.cc_abbreviation.get(j);
				}
			}
			idx = new Random().nextInt(UserPreferences.banners_sizes.size());
	    	name += UserPreferences.banners_sizes.get(idx);
	    	records.get(i).SitePlacementName = new String(name);
		}
    	
    	for(int i=0;i<records.size();i++)
		{
    		if (records.get(i).SiteContentCategory.equals("Mobile")|| UserPreferences.Mobile_keywords.contains(records.get(i).ContentCatGroup))
			{
    			temp = null;
    			idx = records.get(i).SitePlacementName.lastIndexOf(")");
    			if (idx != -1)
    			{
    				temp = new String(records.get(i).SitePlacementName.substring(0, idx+1));
    			}
    			else if (idx == -1)
    			{
    				temp = new String(records.get(i).SitePlacementName);
    			}
    			if ((records.get(i).SitePlacementName.contains("Web") == false)&&(records.get(i).SitePlacementName.contains("App") == false))
    			{
    				if (records.get(i).DeviceOS != null) temp += " "+records.get(i).DeviceOS.toString();
    				if (records.get(i).DeviceViewType != null) temp += "_"+records.get(i).DeviceViewType.toString();
    				records.get(i).SitePlacementName = new String(temp);
    				counter++;
    			}
			}
			
		}
    	return counter ;
    }
	
	@SuppressWarnings("finally")
	public boolean resolve_keywords()
	{
		boolean success = false;
		ArrayList<String> Campaign_Name = new ArrayList<String>() ;
		ArrayList<String> Ad_GroupName = new ArrayList<String>() ;
		ArrayList<String> Keyword = new ArrayList<String>() ;
		ArrayList<Integer> Tracking_ID = new ArrayList<Integer>() ;
		
		String static_query = "SELECT  [Campaign_Name],[Ad_GroupName],[Tracking_ID],[Keyword] FROM [SMGPMDemo].[dbo].[DATA_SearchPerformance]";
				
		try 
        {
            connection = ConnectionFactory.getConnection();
            statement = connection.createStatement();
            resultSet =statement.executeQuery(static_query);
            
            System.out.print(">>Query to retrieve keywords being executed... ");
            while(resultSet .next())
            {
            	if (resultSet.getObject("Campaign_Name") != null) Campaign_Name.add(resultSet.getString("Campaign_Name"));
            	else Campaign_Name.add(new String(NULL));
            	if (resultSet.getObject("Ad_GroupName") != null) Ad_GroupName.add(resultSet.getString("Ad_GroupName"));
            	else Ad_GroupName.add(new String(NULL));
            	if (resultSet.getObject("Keyword") != null) Keyword.add(resultSet.getString("Keyword"));
            	else Keyword.add(new String(NULL));	
            	if (resultSet.getObject("Tracking_ID") != null) Tracking_ID.add(resultSet.getInt("Tracking_ID"));
            	else Tracking_ID.add(-1);
            }
            if (Campaign_Name.size()!= 0) 
            {
            	success = true;
            	System.out.print("Successfully queried "+Campaign_Name.size()+" keywords");
            	if ((Campaign_Name.size() == Ad_GroupName.size()) && (Ad_GroupName.size() == Keyword.size())) System.out.print(" with no problems \n");
            }
            for(int i=0;i<Campaign_Name.size();i++)
    		{
            	Campaign_Name.set(i, new String(Campaign_Name.get(i).replace("Ally", "MyBank"))) ;
    		}
            for(int i=0;i<Ad_GroupName.size();i++)
    		{
            	Ad_GroupName.set(i, new String(Ad_GroupName.get(i).replace("Ally", "MyBank"))) ;
    		}
            for(int i=0;i<Keyword.size();i++)
    		{
            	Keyword.set(i, new String(Keyword.get(i).replace("Ally", "MyBank"))) ;
            	Keyword.set(i, new String(Keyword.get(i).replace("ally", "My"))) ;
    		}
            // -- updating DB
            String updateQuery = "UPDATE [SMGPMDemo].[dbo].[DATA_SearchPerformance] SET "+
                    "Campaign_Name = ?, Ad_GroupName = ?, Keyword = ?"+
                    " WHERE Tracking_ID = ?";
            PreparedStatement update = null;
            update = connection.prepareStatement(updateQuery);
            connection.setAutoCommit(false);
            System.out.println("\n> Updating the keywords database... ");
                    ProgressBar bar = new ProgressBar();
                    int old_value = -1;
                    for(int i=0;i<Keyword.size();i++)
            		{
                        update.setString (1, Campaign_Name.get(i));
                        update.setString (2, Ad_GroupName.get(i));
                        update.setString (3, Keyword.get(i));
                        update.setInt (4, Tracking_ID.get(i));
                        update.executeUpdate();
                        connection.commit();
                        old_value = bar.update(i, old_value, Keyword.size());
                        System.out.print("*");
            		}
                    connection.setAutoCommit(true);
                    success = true;
                    if (success == true) 
                    {
                    	System.out.println("\n>Successfully updated "+Keyword.size()+" keywords \n");
                    }
            SQLWarning warning = statement.getWarnings();
            if (warning != null)
            {
            	warning.printStackTrace();
            	throw new ApplicationException(warning.getMessage());
            }
            
                
        } 
        catch (SQLException e) 
        {
            Tools.printSQLException(e);
        	ApplicationException exception = new ApplicationException(e.getMessage(), e);
            exception.printStackTrace();
            throw exception;
        } 
        finally 
        {
            Tools.close(statement);
            Tools.close(connection);
            Tools.close(resultSet);
            //System.out.println("Query Successfully Executed, successfully got "+queried_values.size()+" of column "+columnName);
            this.sort_by_date();
            return success;
        }
	}
	
	void resolve_products()
	{
		int idx;
		for(int i=0;i<records.size();i++)
		{
			
			if (records.get(i).ProductType.equals(NULL)) 
				{
					idx = new Random().nextInt(UserPreferences.ProductType.size());
					records.get(i).ProductType = new String(UserPreferences.ProductType.get(idx));
				}
			for(int j=0;j<UserPreferences.ProductType.size();j++)
			{
				if(records.get(i).ProductType.equals(UserPreferences.ProductType.get(j)))
				{
					records.get(i).Product = new String(UserPreferences.Product.get(j));
					records.get(i).TotalProduct = new String(UserPreferences.TotalProduct.get(j));
					break;
				}
			}
		}
	}
	
	public void print_consistency_matrix()
	{
		System.out.println("\n>>Printing consistency matrix");
		for(int j=0;j<consistency_matrix.size();j++)
		{
			System.out.println("      "+this.consistency_matrix.get(j).get(0)+"      "+this.consistency_matrix.get(j).get(1)+"       "+this.consistency_matrix.get(j).get(2));
		}
	}
	
	public void update_ContentCatGroup(UserPreferences db, DateTime start, DateTime end)
	{
		ArrayList<Record> indexes = new ArrayList<Record>();
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) indexes.add(records.get(i));
		}
		
		// Modifying percentages
		ArrayList<String> temp_data = new ArrayList<String>();
		for (int i=0;i<indexes.size();i++)
		{
			temp_data.add(indexes.get(i).ContentCatGroup);
		}
		//System.out.println("\n>updating "+indexes.size()+" rows between "+start.toString()+" and "+end.toString()+" ");
		temp_data = Tools.data_updater_from_percentage (temp_data, db.content_category_group_percentage, db.content_category_group);
		// Updating extracted records back
		//System.out.print("\n>updating extracted records with new values...");
		int counter =0;
		//while (counter < temp_data.size())
		//{
			for(int i=0;i<records.size();i++)
			{
				DateTime temp_date = new DateTime(records.get(i).ActivityDate);
				if ((temp_date.isAfter(start))&&(temp_date.isBefore(end))) records.get(i).ContentCatGroup = new String(temp_data.get(counter++));
				if (counter == temp_data.size()) break;
			}
		//}
		//System.out.print(" [OK]\n");
		//System.out.println("\n>update between "+start.toString()+" and "+end.toString()+" [OK]");
		
	}
	
	public void update_pf_dfa_amounts_distribution(UserPreferences db, DateTime start, DateTime end)
	{
		ArrayList<Record> indexes = new ArrayList<Record>();
		
		double total_pf = 0.0 , total_dfa = 0.0 , fraction_pf = 0.0 , fraction_dfa =0.0;
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) 
				{
					indexes.add(records.get(i));
					total_pf += records.get(i).Attribution;
					total_dfa += records.get(i).DFA_Accounts;
				}
		}
		// fraction to calculate
		fraction_pf = (double) total_pf / indexes.size();
		fraction_dfa = (double) total_dfa / indexes.size();
		
		// Modifying percentages
		for (int i=0;i<indexes.size();i++)
		{
			for (int j=0;j<UserPreferences.content_category.size();j++)
			{
				if(indexes.get(i).SiteContentCategory.equals(UserPreferences.content_category.get(j)))
				{
					indexes.get(i).Attribution += fraction_pf * UserPreferences.pf_variation_factor.get(j);
					indexes.get(i).DFA_Accounts += fraction_dfa * UserPreferences.dfa_variation_factor.get(j);
				}
			}
		}
		// Updating extracted records back
		//System.out.print("\n>updating extracted records with new values...");
		int counter =0;
		//while (counter < temp_data.size())
		//{
			for(int i=0;i<records.size();i++)
			{
				DateTime temp_date = new DateTime(records.get(i).ActivityDate);
				if ((temp_date.isAfter(start))&&(temp_date.isBefore(end))) 
					{
						records.get(i).Attribution = indexes.get(counter).Attribution;
						records.get(i).DFA_Accounts = indexes.get(counter++).DFA_Accounts;
					}
				if (counter == indexes.size()) break;
			}
		//}
		//System.out.print(" [OK]\n");
		//System.out.println("\n>update between "+start.toString()+" and "+end.toString()+" [OK]");
		
	}
	
	public void update_ContentCategory(UserPreferences db, DateTime start, DateTime end)
	{
		ArrayList<Record> indexes = new ArrayList<Record>();
		
		
    	int counter_specialCase = 0 ;
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) indexes.add(records.get(i));
		}
		// ---- Special case: Frontpage pick on January through March
		//-----------------------------------------------------------
		//-----------------------------------------------------------
		DateTime start_specialCase =   new DateTime(2014, 1, 15, 0, 0);
    	DateTime end_specialCase =     new DateTime(2014, 6, 30, 0, 0, 0);
    	DateTime start_specialCase2 =   new DateTime(2013, 12, 10, 0, 0);
    	DateTime end_specialCase2 =     new DateTime(2014, 1, 15, 0, 0, 0);
		// Modifying percentages
		ArrayList<String> temp_data = new ArrayList<String>();
		for (int i=0;i<indexes.size();i++)
		{
			temp_data.add(indexes.get(i).SiteContentCategory);
		}
		//System.out.println("\n>updating "+indexes.size()+" rows between "+start.toString()+" and "+end.toString()+" ");
		
		if( (start.isAfter(start_specialCase)) && (end.isBefore(end_specialCase)))
		{
			temp_data = Tools.data_updater_from_percentage (temp_data, db.content_category_percentage_specialCase, db.content_category); counter_specialCase++;
		}
		else if( (start.isAfter(start_specialCase2)) && (end.isBefore(end_specialCase2)))
		{
			temp_data = Tools.data_updater_from_percentage (temp_data, db.cc_perc_sc2, db.content_category); counter_specialCase++;
		}
		else
		{
			temp_data = Tools.data_updater_from_percentage (temp_data, db.content_category_percentage, db.content_category);
		}
		
		// Updating extracted records back
		//System.out.print("\n>updating extracted records with new values...");
		int counter =0;
		//while (counter < temp_data.size())
		//{
			for(int i=0;i<records.size();i++)
			{
				DateTime temp_date = new DateTime(records.get(i).ActivityDate);
				if ((temp_date.isAfter(start))&&(temp_date.isBefore(end))) records.get(i).SiteContentCategory = new String(temp_data.get(counter++));
				if (counter == temp_data.size()) break;
			}
		//}
		//System.out.print(" [OK]\n");
		System.out.println("\n>update between "+start.toString()+" and "+end.toString()+" with "+counter_specialCase+" special case");
		
	}
	
	public int update_products(UserPreferences db, DateTime start, DateTime end)
	{
		ArrayList<Record> indexes = new ArrayList<Record>();
		
		DateTime start_specialCase =   new DateTime(2014, 1, 15, 0, 0);
    	DateTime end_specialCase =     new DateTime(2014, 5, 15, 0, 0, 0);
    	int counter_specialCase = 0 ;
    	int records_counter = 0 ;
    	int idx ;
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) indexes.add(records.get(i)); records_counter++;
		}
		// ---- Special case: Frontpage pick on January through March
		//-----------------------------------------------------------
		//-----------------------------------------------------------
		
		// Modifying percentages
		ArrayList<String> temp_data = new ArrayList<String>();
		for (int i=0;i<indexes.size();i++)
		{
			temp_data.add(indexes.get(i).ProductType);
		}
		//System.out.println("\n>updating "+indexes.size()+" rows between "+start.toString()+" and "+end.toString()+" ");
		
		if( (start.isAfter(start_specialCase)) && (end.isBefore(end_specialCase)))
		{
			idx = new Random().nextInt(UserPreferences.ProductType_percentage.size());
			
			temp_data = Tools.data_updater_from_percentage (temp_data, UserPreferences.ProductType_percentage.get(idx), UserPreferences.ProductType); counter_specialCase++;
		}
		else
		{
			idx = new Random().nextInt(UserPreferences.ProductType_percentage.size());
			temp_data = Tools.data_updater_from_percentage (temp_data, UserPreferences.ProductType_percentage.get(idx), UserPreferences.ProductType);
		}
		
		// Updating extracted records back
		//System.out.print("\n>updating extracted records with new values...");
		int counter =0;
		//while (counter < temp_data.size())
		//{
			for(int i=0;i<records.size();i++)
			{
				DateTime temp_date = new DateTime(records.get(i).ActivityDate);
				if ((temp_date.isAfter(start))&&(temp_date.isBefore(end))) records.get(i).ProductType = new String(temp_data.get(counter++));
				for(int j=0;j<UserPreferences.ProductType.size();j++)
				{
					if(records.get(i).ProductType.equals(UserPreferences.ProductType.get(j)))
					{
						records.get(i).Product = new String(UserPreferences.Product.get(j));
						records.get(i).TotalProduct = new String(UserPreferences.TotalProduct.get(j));
						break;
					}
				}
				if (counter == temp_data.size()) break;
			}
		//}
		//System.out.print(" [OK]\n");
		System.out.println("\n>update between "+start.toString()+" and "+end.toString()+" with "+counter_specialCase+" special case");
		return records_counter;
	}
	
	public int update_mobile(UserPreferences db, DateTime start, DateTime end)
	{
		ArrayList<Record> indexes = new ArrayList<Record>();
		
		int mobile_counter = 0 ;
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end)))
			{
				if (records.get(i).SiteContentCategory.equals("Mobile")|| UserPreferences.Mobile_keywords.contains(records.get(i).ContentCatGroup))
					{
						mobile_counter++;
						indexes.add(records.get(i));
					}
			}
		}
		
		ArrayList<String> MobileDevice_temp = new ArrayList<String>();
		ArrayList<String> DeviceViewType_temp = new ArrayList<String>();
		
		for (int i=0;i<indexes.size();i++)
		{
			MobileDevice_temp.add(indexes.get(i).MobileDevice);
			DeviceViewType_temp.add(indexes.get(i).DeviceViewType);
		}
		//System.out.println("\n>updating "+indexes.size()+" rows between "+start.toString()+" and "+end.toString()+" ");
		
		MobileDevice_temp = Tools.data_updater_from_percentage (MobileDevice_temp, db.MobileDevice_percentage, db.MobileDevice);
		DeviceViewType_temp = Tools.data_updater_from_percentage (DeviceViewType_temp, db.DeviceViewType_percentage, db.DeviceViewType);
		
		// Updating extracted records back
		//System.out.print("\n>updating extracted records with new values...");
		int counter =0;
			for(int i=0;i<records.size();i++)
			{
				DateTime temp_date = new DateTime(records.get(i).ActivityDate);
				if ((temp_date.isAfter(start))&&(temp_date.isBefore(end))) 
				{
					if (records.get(i).SiteContentCategory.equals("Mobile")|| UserPreferences.Mobile_keywords.contains(records.get(i).ContentCatGroup)) 
					{
							records.get(i).MobileDevice = new String(MobileDevice_temp.get(counter));
							records.get(i).DeviceViewType = new String(DeviceViewType_temp.get(counter++));
					}
				}
					
				if (counter == DeviceViewType_temp.size()) break;
			}
		// dealing with DeviceOS
			int idx = 0 ;
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end)))
			{
				if (records.get(i).MobileDevice.equals("Smartphone")) 
				{
					idx = new Random().nextInt(UserPreferences.DeviceOS_smartphone.size());
					records.get(i).DeviceOS = new String(UserPreferences.DeviceOS_smartphone.get(idx));
				}
			}
			else if (records.get(i).MobileDevice.equals("Tablet")) 
			{
				idx = new Random().nextInt(UserPreferences.DeviceOS_tablet.size());
				records.get(i).DeviceOS = new String(UserPreferences.DeviceOS_tablet.get(idx));
			}
		}
		
		System.out.println("\n>update between "+start.toString()+" and "+end.toString()+" with "+counter+" changes");
		return mobile_counter;
		
	}
	
	public int count_rows(UserPreferences db, DateTime start, DateTime end)
	{
		int counter = 0 ;
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end))) counter++;
		}
		System.out.println("\n>there are "+counter+" rows between "+start.toString()+" and "+end.toString()+" ");
		return counter;
	}
	
	public void update_ROAS_by_ContentCategory(UserPreferences db, DateTime start, DateTime end, String SiteContentCategory, double ffraction, boolean add)
	{
		
		ArrayList<Integer> positions = new ArrayList<Integer>();
		
		positions.add(-10);
		positions.add(-9);
		positions.add(-8);
		positions.add(-7);
		positions.add(-6);
		positions.add(-5);
		positions.add(-4);
		positions.add(-3);
		positions.add(-2);
		positions.add(-1);
		positions.add(1);
		positions.add(2);
		positions.add(3);
		positions.add(4);
		positions.add(5);
		positions.add(6);
		positions.add(7);
		positions.add(8);
		positions.add(9);
		positions.add(10);
		
		int idx ;
		
		Random r = new Random();
		
		double randomValue = ffraction;
		//System.out.print("\n>updating extracted records with new percentage...");
		int counter = 0 ;
		int randomInt = 0 ;
		double fraction_counter = 0.0 ;
		double fraction = 0.0 ;
		
		for(int i=0;i<records.size();i++)
		{
			DateTime temp_date = new DateTime(records.get(i).ActivityDate);
			if ((temp_date.isAfter(start))&&(temp_date.isEqual(end)|| temp_date.isBefore(end)) && (records.get(i).SiteContentCategory.equals(SiteContentCategory))) 
				{
						if (add == true)
						{
							fraction = new Double(records.get(i).Profitability*randomValue);
							records.get(i).Profitability += fraction;
							fraction_counter += records.get(i).Profitability;
							counter++;
							// determining position to update the other raw
							do
							{
								idx = new Random().nextInt(positions.size());
							}
							while ( ((i+positions.get(idx)) < 0) || ((i+positions.get(idx)) >= records.size()) );
							// update the neighbor row
							records.get(i+positions.get(idx)).Profitability -= fraction;
						}
						else
						{
							fraction = new Double(records.get(i).Profitability*randomValue);
							records.get(i).Profitability -= fraction;
							fraction_counter += records.get(i).Profitability;
							counter++;
							// determining position to update the other raw
							do
							{
								idx = new Random().nextInt(positions.size());
							}
							while ( (((i+positions.get(idx)) < 0) || ((i+positions.get(idx)) >= records.size())) );
							// update the neighbor row
							if ((!records.get(i+positions.get(idx)).SiteContentCategory.equals(SiteContentCategory))&&(!records.get(i+positions.get(idx)).SiteContentCategory.equals("Social Retargeting")))records.get(i+positions.get(idx)).Profitability += fraction;
						}
				}
		}
		//System.out.print(" [OK]\n");
		//System.out.println("\n> "+counter+" rows have been updated between "+start.toString()+" and "+end.toString()+" with an average of "+(fraction_counter/counter));
	}
	
	void update_values_byPercentage(String field, double percentage)
	{
		//double percentage = 0.2 ;
		double fraction = 0 ;
		System.out.print("\n Updating the field "+field+" by adding "+percentage+" percentage to it...");
		if(field.equals("EV_1"))
		{
			for(int i=0;i<records.size();i++)
			{
				fraction = (double) records.get(i).Profitability * percentage ;
				records.get(i).Profitability += fraction ;
			}
		}
		else if (field.equals("DFASumClicks"))
		{
			for(int i=0;i<records.size();i++)
			{
				fraction = (double) records.get(i).DFASumClicks * percentage ;
				records.get(i).DFASumClicks += fraction ;
			}
		}
		else if (field.equals("DFASumImps"))
		{
			for(int i=0;i<records.size();i++)
			{
				fraction = (double) records.get(i).DFASumImps * percentage ;
				records.get(i).DFASumImps += fraction ;
			}
		}
		else if (field.equals("MediaCost"))
		{
			for(int i=0;i<records.size();i++)
			{
				fraction = (double) records.get(i).MediaCost * percentage ;
				records.get(i).MediaCost += fraction ;
			}
		}
		else if (field.equals("EstimatedBalance_1"))
		{
			for(int i=0;i<records.size();i++)
			{
				fraction = (double) records.get(i).EstimatedBalance_1 * percentage ;
				records.get(i).EstimatedBalance_1 += fraction ;
			}
		}
		else if (field.equals("DFA_Accounts"))
		{
			for(int i=0;i<records.size();i++)
			{
				fraction = (double) records.get(i).DFA_Accounts * percentage ;
				records.get(i).DFA_Accounts += fraction ;
			}
		}
		System.out.print(" [OK]\n");
	}
	
	public void update_Accounts_number()
	{
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime int0 =    new DateTime(2013, 6, 30, 0, 0);
    	DateTime int1 =    new DateTime(2013, 7, 30, 0, 0);
    	DateTime int2 =    new DateTime(2013, 10, 30, 0, 0);
    	DateTime int3 =    new DateTime(2014, 1, 30, 0, 0);
    	DateTime int4 =    new DateTime(2014, 3, 30, 0, 0);
    	DateTime int5 =    new DateTime(2014, 4, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 6, 30, 12, 0, 0);
    	
    	this.set_array_values_on_period("Attribution", 0.060, 0.070, -0.02, +0.02, start, int0);
    	this.set_array_values_on_period("Attribution", 0.070, 0.080, -0.01, +0.01, int0, int1);
    	this.set_array_values_on_period("Attribution", 0.080, 0.050, -0.02, +0.02, int1, int2);
    	this.set_array_values_on_period("Attribution", 0.050, 0.060, -0.01, +0.01, int2, int3);
    	this.set_array_values_on_period("Attribution", 0.060, 0.090, -0.01, +0.01, int3, int4);
    	this.set_array_values_on_period("Attribution", 0.085, 0.085, -0.01, +0.01, int4, int5);
    	this.set_array_values_on_period("Attribution", 0.085, 0.090, -0.01, +0.02, int5, end);
    	
    	this.update_DFAaccounts_DFAdeposits();
	}
	
	public void update_clicks_impressions()
	{
		/*
		for(int i=0;i<records.size();i++)
		{
			records.get(i).DFASumClicks = (records.get(i).DFASumClicks < 0 ? -records.get(i).DFASumClicks : records.get(i).DFASumClicks);
			records.get(i).DFASumImps = (records.get(i).DFASumImps < 0 ? -records.get(i).DFASumImps : records.get(i).DFASumImps);
		}
		*/
		/*
		for(int i=0;i<records.size();i++)
		{
			records.get(i).DFASumClicks = 100.0;
			records.get(i).DFASumImps = 1000.0;
		}
		*/
		DateTime start =   new DateTime(2013, 5, 30, 0, 0);
    	DateTime int0 =    new DateTime(2013, 6, 30, 0, 0);
    	DateTime int1 =    new DateTime(2013, 7, 30, 0, 0);
    	DateTime int2 =    new DateTime(2013, 10, 30, 0, 0);
    	DateTime int3 =    new DateTime(2014, 1, 30, 0, 0);
    	DateTime int4 =    new DateTime(2014, 3, 30, 0, 0);
    	DateTime int5 =    new DateTime(2014, 4, 30, 0, 0);
    	DateTime end =     new DateTime(2014, 5, 30, 12, 0, 0);
    	/*
    	this.set_array_values_on_period("DFASumImps", 2000.0, 2200.0, -5.0, +5.0, start, int0);
    	this.set_array_values_on_period("DFASumImps", 2200.0, 2000.0, -5.0, +5.0, int0, int1);
    	this.set_array_values_on_period("DFASumImps", 2000.0, 1500.0, -5.0, +5.0, int1, int2);
    	this.set_array_values_on_period("DFASumImps", 1500.0, 1400.0, -5.0, +5.0, int2, int3);
    	this.set_array_values_on_period("DFASumImps", 1600.0, 2200.0, -5.0, +5.0, int3, int4);
    	this.set_array_values_on_period("DFASumImps", 2200.0, 2500.0, -5.0, +5.0, int4, int5);
    	this.set_array_values_on_period("DFASumImps", 2500.0, 2600.0, -5.0, +5.0, int5, end);
    	*/
    	this.set_array_values_on_period("DFASumClicks", 70.0, 80.0, -1.0, +1.0, start, int0);
    	this.set_array_values_on_period("DFASumClicks", 80.0, 100.0, -1.0, +1.0, int0, int1);
    	this.set_array_values_on_period("DFASumClicks", 100.0, 110.0, -1.0, +1.0, int1, int2);
    	this.set_array_values_on_period("DFASumClicks", 110.0, 120.0, -1.0, +1.0, int2, int3);
    	this.set_array_values_on_period("DFASumClicks", 120.0, 140.0, -1.0, +1.0, int3, int4);
    	this.set_array_values_on_period("DFASumClicks", 140.0, 150.0, -1.0, +1.0, int4, int5);
    	this.set_array_values_on_period("DFASumClicks", 150.0, 160.0, -1.0, +1.0, int5, end);	
	}
	
	public void update_DFAaccounts_DFAdeposits()
	{
		// DFA account should be almost equal to Attribution
    	for(int i=0;i<records.size();i++)
		{
			records.get(i).DFA_Accounts = records.get(i).Attribution;
			records.get(i).ActivityRevenue_1 = records.get(i).EstimatedBalance_1;
			records.get(i).DFA_Revenue = records.get(i).EstimatedBalance_1;
		}
	}
}
