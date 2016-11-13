package db;

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
    
 }

