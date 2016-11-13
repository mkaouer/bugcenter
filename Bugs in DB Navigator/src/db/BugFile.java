package db;

import java.util.ArrayList;

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
					this.bug_dates.add(Integer.toString(bugs.get(j).report_time));
					this.bug_date.concat(" "+Integer.toString(bugs.get(j).report_time.getYear()));
				}
			}
		    
		}
	}
	
	void printData()
	{
		System.out.println("doc_id:"+this.doc_id+" has:"+this.bug_ids.size()+
				" bugs and it name is:"+this.file_name);
	}

}
