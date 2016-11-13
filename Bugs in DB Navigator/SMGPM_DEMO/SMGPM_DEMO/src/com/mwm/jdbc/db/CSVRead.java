package com.mwm.jdbc.db;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class CSVRead{

 public static ArrayList<String> read_csv(Records p) throws Exception 
 {

  BufferedReader CSVFile = new BufferedReader(new FileReader("SitePlacementName.csv"));

  ArrayList<String> results = new ArrayList<String>(); 
  String dataRow = CSVFile.readLine(); // Read first line.
  // The while checks to see if the data is null. If 
  // it is, we've hit the end of the file. If not, 
  // process the data.

  while (dataRow != null){
   String[] dataArray = dataRow.split(",");
   results.add(dataRow);
   //for (String item:dataArray) { 
    //  System.out.print(item + "\t"); 
   //}
   //System.out.println(); // Print the data line.
   dataRow = CSVFile.readLine(); // Read next line of data.
  }
  // Close the file once all data has been read.
  CSVFile.close();

  // End the printout with a blank line.
  //System.out.println();
  
  for (int i = 0 ; i < p.records.size(); i++)
  {
  	p.records.get(i).SitePlacementName = new String (results.get(i));
  }
  
  System.out.println("we have "+p.records.size()+" records and "+results.size()+" data used to update");
  
  return results;
 } //main()
} // CSVRead