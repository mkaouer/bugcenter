
public class Bug 
{
	int bug_id;
	String summary;
	String description;
	String report_time;
	java.sql.Timestamp report_timestamp;
	java.sql.Timestamp commit_timestamp;
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
