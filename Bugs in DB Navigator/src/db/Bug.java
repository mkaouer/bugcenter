package db;

public class Bug 
{
	int bug_id;
	String summary;
	String description;
	String report_time;
	Double report_timestamp;
	Double commit_timestamp;
	String files;
	
	public Bug()
	{
		
	}
	
	void printData()
	{
		System.out.println("\n bug_id:"+this.bug_id+" start:"+this.report_time.toString()+
				" fixed:"+this.commit_timestamp.toString());
	}
	
}
