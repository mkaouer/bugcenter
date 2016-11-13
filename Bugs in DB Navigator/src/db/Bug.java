package db;

public class Bug 
{
	int bug_id;
	String summary;
	String description;
	java.sql.Date report_time;
	String report_time_2;
	Double report_timestamp;
	Double commit_timestamp;
	String files;
	
	public Bug()
	{
		this.summary = new String();
		this.description = new String();
		this.report_time = new java.sql.Date(0);
		this.report_time_2 = new String();
	}
	
		
	void printData()
	{
		System.out.println("bug_id:"+this.bug_id+" start:"+this.report_time_2.toString()+
				" fixed:"+this.commit_timestamp.toString());
	}
	
}
