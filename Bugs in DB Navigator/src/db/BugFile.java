package db;

import java.util.ArrayList;

public class BugFile 
{
	int doc_id;
	String file_name;
	String bug_id;
	ArrayList<String> bug_ids; 
	
	BugFile()
	{
		doc_id = -1;
		file_name = new String();
		bug_id = new String();
		bug_ids = new ArrayList<String>();
	}
	
	

}
