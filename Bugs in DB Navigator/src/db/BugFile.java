package db;

import java.util.ArrayList;
import java.util.Calendar;

public class BugFile 
{
	int doc_id;
	String file_name;
	String bug_id;
	String bug_date;
	ArrayList<String> bug_ids;
	ArrayList<String> bug_dates;
	ArrayList<Bug> bugs;
	
	BugFile()
	{
		doc_id = -1;
		file_name = new String();
		bug_id = new String();
		bug_date = new String();
		bug_ids = new ArrayList<String>();
		bug_dates = new ArrayList<String>();
		this.bugs = new ArrayList();
	}
	
	void split_bug_ids(ArrayList<Bug> bugs)
	{
		String[] splitStr = bug_id.split("\\s+");
		for (int i = 0; i < splitStr.length; i++) 
		{
			bug_ids.add(splitStr[i]);
		}
		for (int i = 0; i < bug_ids.size(); i++) 
		{
			for (int j = 0; j < bugs.size(); j++) 
			{
				if(bugs.get(j).bug_id == Integer.parseInt(bug_ids.get(i)))
				{
					this.bugs.add(bugs.get(j));
					this.bug_dates.add(bugs.get(j).report_time_2.substring(0,10));					
					//create calander instance and get required params
					
					Calendar cal = Calendar.getInstance();
					cal.setTime(bugs.get(j).report_time);
					int month = cal.get(Calendar.MONTH);
					int day = cal.get(Calendar.DAY_OF_MONTH);
					int year = cal.get(Calendar.YEAR);
					//this.bug_dates.add(Integer.toString(year));
					//this.bug_date.concat(" "+Integer.toString(year));
					
				}
			} 
		}
		this.bug_date = new String();
		
		for (int i = 0; i < this.bug_dates.size(); i++) 
		{
			if (i==0) this.bug_date = new String(bug_dates.get(i));
			else this.bug_date = this.bug_date.concat(" "+bug_dates.get(i));
		}
	}
	
	void printData()
	{
		System.out.println("doc_id:"+this.doc_id+" has:"+this.bug_ids.size()+
				" bugs reported and "+this.bugs.size()+" bugs detected and "+this.bug_dates.size()+" years detected:");
		for (int i=0 ; i < this.bug_dates.size() ; i++)
		{
			System.out.print(bug_dates.get(i)+" ");
		}
		System.out.println();
	}

}
