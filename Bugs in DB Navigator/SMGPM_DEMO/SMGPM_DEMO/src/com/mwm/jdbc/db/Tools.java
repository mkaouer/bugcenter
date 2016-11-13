package com.mwm.jdbc.db;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;

import org.joda.time.DateTime;
 
public class Tools 
{
 
    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                /*Ignore*/
            }
        }
    }
 
    public static void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                /*Ignore*/
            }
        }
    }
 
    public static void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                /*Ignore*/
            }
        }
    }
    
    public static ArrayList<Double> Multiplier(ArrayList<Double> numbers, Double multiplier)
    {
    	ArrayList<Double> results = new ArrayList<Double>();
    	
    	for(int i=0;i<numbers.size();i++)
        {
            results.add((Double)numbers.get(i)* multiplier);
        }
    	
    	return results;
    }
    
    public static ArrayList<Double> null2zero(ArrayList<Double> numbers)
    {
    	ArrayList<Double> results = new ArrayList<Double>();
    	
    	for(int i=0;i<numbers.size();i++)
        {
            if (numbers.get(i) == null) results.add(0.0);
            else results.add(numbers.get(i));
        }
    	
    	return results;
    }
    
    public static Double average_value(ArrayList<Double> numbers)
    {
    	Double result = 0.0;
    	
    	Double sum = 0.0 ;
    	
    	for(int i=0;i<numbers.size();i++)
        {
            sum+= numbers.get(i);
            
        }
    	if (sum != 0) result = sum / numbers.size();
    	
    	return result;
    }
    
    public static Double min_value(ArrayList<Double> numbers)
    {
    	Double result = Double.MAX_VALUE;
    	
    	for(int i=0;i<numbers.size();i++)
        {
            if (result > numbers.get(i)) result = numbers.get(i);
            
        }
    	
    	return result;
    }
    
    public static Double max_value(ArrayList<Double> numbers)
    {
    	Double result = 0.0;
    	
    	for(int i=0;i<numbers.size();i++)
        {
            if (result < numbers.get(i)) result = numbers.get(i);
            
        }
    	
    	return result;
    }
    
    public static ArrayList<Double> random_walk_generator(int size, Double min_target, Double min_random_factor, Double max_random_factor)
    {
    	ArrayList<Double> results = new ArrayList<Double>();
    	Random r = new Random();
    	Double factor = 0.0;
    	
    	results.add(min_target);
    	
    	for(int i=1;i<size;i++)
        {
    		factor = min_random_factor + (max_random_factor - min_random_factor) * r.nextDouble();
        		
    		results.add(0.0); // just to allocate the memory
    			
    		results.set(i, results.get(i-1) + factor ) ;
    			
        }
    	
    	//results.add(max_target);
    	
    	return results;
    }
    
    public static ArrayList<Double> progressive_values_generator(int size, Double min_target, Double max_target, Double min_random_factor, Double max_random_factor)
    {
    	ArrayList<Double> results = new ArrayList<Double>();
    	Random r = new Random();
    	//boolean acceptable_value = false;
    	
    	Double factor = 0.0;
    	Double step = (max_target - min_target)/size;
    	System.out.println(">Progressive walk initiated with step "+step);
    	results.add(min_target);
    	
    	for(int i=1;i<size;i++)
        {
    		factor = min_random_factor + (max_random_factor - min_random_factor) * r.nextDouble();
        		
    			results.add(0.0); // just to allocate the memory
    			
    			results.set(i, min_target+ step*i + factor ) ;
    			
    			if (results.get(i) < min_target)
    			{
    				//System.out.println(" Warning: Underflow detected on progressive values generator on the iteration "+i+" ---");
    			}
    				
    			if (results.get(i) > max_target)
    			{
    				//System.out.println(" Warning: Overflow detected on progressive values generator on the iteration "+i+" ---");
    			}
    				
    		}
    	//results.add(max_target);
    	
    	return results;
    }
    
    public static ArrayList duplicates_remover(ArrayList duplicateList)
    {
    	//ArrayList with duplicates String
    	//List<String> duplicateList = (List<String>) Arrays.asList("Android" , "Android", "iOS", "Windows mobile");
    	//System.out.println("size of ArrayList with duplicates: " + duplicateList.size()); //should print 4 because of duplicates Android

    	//System.out.println(duplicateList);
    	     
    	//Converting ArrayList to HashSet to remove duplicates
    	HashSet listToSet = new HashSet(duplicateList);
    	     
    	//Creating ArrayList without duplicate values
    	ArrayList listWithoutDuplicates = new ArrayList(listToSet);
    	System.out.println("size of ArrayList of dates with duplicates: "+duplicateList.size()+" without duplicates: " + listWithoutDuplicates.size()); //should print 3 because of duplicates Android removed

    	//Read more: http://javarevisited.blogspot.com/2012/12/how-to-remove-duplicates-elements-from-ArrayList-Java.html#ixzz36EGlc6Sk

    	
    	return listWithoutDuplicates;
    	
    }
    
    public static void print_array_values (ArrayList list)
    {
    	System.out.println("\n --- Printing Array  ---");
        
        for(int i=0;i<list.size();i++)
        {
            System.out.println(list.get(i));
        }
    }
    
    public static double median_value (ArrayList<Double> data)
    {
        double res = 0.0;
        
    	for(int i=0;i<data.size();i++)
        {
            res += data.get(i);
        }
    	res /= data.size();
    	
    	return res ;
    }
    
    public static void print_array_bounds(ArrayList list)
    {
    	System.out.println("\n --- Printing Array bounds of array which has size ---"+list.size());
        
    	System.out.println(list.get(0));
    	System.out.println(list.get(1));
    	System.out.println(list.get(list.size()-2));
    	System.out.println(list.get(list.size()-1));
    }
    
    public static void print_content_name_percentage (ArrayList<String> content, ArrayList<Double> percentage, String field)
    {
    	System.out.println("\n ***--- Printing information for field "+field+" ---***");
        
        for(int i=0;i<content.size();i++)
        {
            System.out.println(" - "+content.get(i)+" - "+percentage.get(i));
        }
    }
    
    public static boolean percentage_checksum (ArrayList<Double> list)
    {
    	boolean OK = true;
    	Double temp_sum = 0.0 ;
        
        for(int i=0;i<list.size();i++)
        {
            temp_sum += list.get(i);	
        }
        
        temp_sum = round(temp_sum, 2);
        
        if (temp_sum == 1.0) 
        	{
        		OK = true;
        		System.out.println("Percentage checksum [OK]");
        	}
        else
        {
        	OK = false;
        	System.out.println("Problem with percentage checksum detected as its value is "+temp_sum);
        }
        return OK;
    }
    
    public static boolean zero_checksum (ArrayList<Double> list)
    {
    	boolean OK = true;
    	Double temp_sum = 0.0 ;
        
        for(int i=0;i<list.size();i++)
        {
            temp_sum += list.get(i);	
        }
        temp_sum = round(temp_sum, 5);
        
        if (temp_sum == 0.0) 
        	{
        		OK = true;
        		System.out.println("Zero checksum [OK]");
        	}
        else
        {
        	OK = false;
        	System.out.println("Problem with Zero checksum detected as its value is "+temp_sum.toString());
        }
        return OK;
    }
    
    public static ArrayList<String> data_updater_from_percentage (ArrayList<String> content, ArrayList<Double> percentage, ArrayList<String> content_headers )
    {
    	ArrayList<String> result = new ArrayList<String>(content);
    	ArrayList<Integer> content_index = new ArrayList<Integer>();
    	boolean no_overflow = true;
    	
    	System.out.println("\n>>updating data from percentages. data size: "+content.size()+" percentage size: "+percentage.size()+" headers size: "+content_headers.size());
        
        for(int i=0;i<content.size();i++)
        {
            content_index.add(i);
        }
        int overflow_counter = 0 ;
        
        for(int i=0;i<percentage.size();i++)
        {
            int number_rows = (int) Math.round(content.size()* percentage.get(i));
            //System.out.println(">For "+content_headers.get(i)+" with percentage "+percentage.get(i)+" the needed rows are "+number_rows);
            int chosen_index = 0;
            Random r = new Random();
            
            
            for(int j=0;j<number_rows;j++)
            {
            	//progress_counter++;
            	
            	if (content_index.size()>0 )
            	{
            		chosen_index = Randomness.random(0,content_index.size()-1);
                	
                	result.set(content_index.get(chosen_index), content_headers.get(i));
                	
                	content_index.remove(chosen_index);
            	}
            	else
            	{
            		//System.out.print("\n>warning: no more available rows. underflow: ");
            		no_overflow = false;
            		overflow_counter++;
            	}
            		
            }        
        }
        //System.out.print(overflow_counter);
     // fixing possible out of bounds
        if (content.size()>result.size())
    	{
        	int remaining =  content.size()-result.size();
        	int chosen_index = 0;
            Random r = new Random();
            //System.out.println(">>adding "+remaining+" rows to the set");
            for(int i=0;i<remaining;i++)
            {
            	chosen_index = r.nextInt(content_headers.size());
            	result.add(content_headers.get(chosen_index));
            }  
    	}
        
        if (content.size()<result.size())
    	{
        	int extra =  result.size()- content.size();
        	int chosen_index = 0;
            Random r = new Random();
            //System.out.println(">>removing "+extra+" rows from the set");
            for(int i=0;i<extra;i++)
            {
            	chosen_index = r.nextInt(result.size());
            	result.remove(chosen_index);
            }  
    	}
        //System.out.println("\n>returning updated data with size "+result.size());
        return result;
    }
    /*
    private static BigDecimal truncateDecimal(double x,int numberofDecimals)
    {
        if ( x > 0) {
            return new BigDecimal(String.valueOf(x)).setScale(numberofDecimals, BigDecimal.ROUND_FLOOR);
        } else {
            return new BigDecimal(String.valueOf(x)).setScale(numberofDecimals, BigDecimal.ROUND_CEILING);
        }
    }
    */
    public static double round(double d, int decimalPlace) 
    {
    	if (decimalPlace < 0) throw new IllegalArgumentException();
    	BigDecimal bd = new BigDecimal(d);
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }
    
    public static void printSQLException(SQLException ex) 
    {

        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                if (ignoreSQLException(
                    ((SQLException)e).
                    getSQLState()) == false) {

                    e.printStackTrace(System.err);
                    System.err.println("SQLState: " +
                        ((SQLException)e).getSQLState());

                    System.err.println("Error Code: " +
                        ((SQLException)e).getErrorCode());

                    System.err.println("Message: " + e.getMessage());

                    Throwable t = ex.getCause();
                    while(t != null) {
                        System.out.println("Cause: " + t);
                        t = t.getCause();
                    }
                }
            }
        }
    }
    
    public static boolean ignoreSQLException(String sqlState) 
    {

        if (sqlState == null) {
            System.out.println("The SQL state is not defined!");
            return false;
        }

        // X0Y32: Jar file already exists in schema
        if (sqlState.equalsIgnoreCase("X0Y32"))
            return true;

        // 42Y55: Table already exists in schema
        if (sqlState.equalsIgnoreCase("42Y55"))
            return true;

        return false;
    }
    
    public static int update_sites_per_cc(String cc, ArrayList<String> websites, ArrayList<Record> records)
    {
    	int idx ;
    	int counter = 0 ;
    	for(int i=0;i<records.size();i++)
		{
			if ( records.get(i).SiteContentCategory.equals(cc)) 
			{
				idx = new Random().nextInt(websites.size());
				records.get(i).Site_Name = new String(websites.get(idx));
				records.get(i).DFA_Site_Name = new String(websites.get(idx));
				counter++;
			}
		}
    	return counter;
    }
    
    
    
    public static void add_one_year(ArrayList<Record> records)
    {
    	for(int i=0;i<records.size();i++)
		{
    		DateTime temp_date = new DateTime(records.get(i).ActivityDate);
    		temp_date.plusYears(1);
    		records.get(i).ActivityDate = new Date();
    		records.get(i).ActivityDate = temp_date.toDate();
		}
    }
    
    public static int fix_IPad(Records p)
    {
    	int counter = 0 ;
    	for(int i=0;i<p.records.size();i++)
		{
			if (p.records.get(i).SiteContentCategory.equals("IPad"))
			{
				p.records.get(i).SiteContentCategory = new String("Ipad");
				counter++;
			}
		}
    	System.out.println(">number of fixed invalid IPad: "+counter);
    	return counter;
    }
    
    public static int move_frontpage_to_awareness(Records p)
    {
    	int counter = 0 ;
    	for(int i=0;i<p.records.size();i++)
		{
			if (p.records.get(i).SiteContentCategory.equals("Frontpage"))
			{
				p.records.get(i).Placement_Group_Strategy = new String("Awareness");
				counter++;
			}
		}
    	System.out.println(">number of moved Frontpage records to awareness: "+counter);
    	return counter;
    }
}

