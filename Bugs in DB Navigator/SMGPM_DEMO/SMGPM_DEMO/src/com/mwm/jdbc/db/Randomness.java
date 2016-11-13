package com.mwm.jdbc.db;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

public class Randomness 
{
			
	public static int random(int min, int max)
	{
		
    	int x=(int)(Math.random()*100);
    	if(max-min<=1 || max==0)
    		return 0;
    	while (x<min || x>max)
    	{
    		x=(int)(Math.random()*100);
    		//System.out.println(x+".");	
    	}
    	return x;
    }
	
	public static String randomClass(Vector classes)
	{
		//Vector detectedBlocs=new Vector();
		
		String res=new String();
		int x=random(0,classes.size()-1);
		res=(String)classes.elementAt(x);
		
		return res;
	}
	
	

}
