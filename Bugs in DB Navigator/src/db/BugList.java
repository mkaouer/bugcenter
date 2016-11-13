package db;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;


public class BugList 
{
	ArrayList<Bug> bugs;
	ArrayList<BugFile> files;
	// Connection Parameters
	private Connection connection;
	private Statement statement;
	private ResultSet resultSet;
	
	BugList()
	{
		this.bugs = new ArrayList();
		this.files = new ArrayList();
	}
		   

	
	void printData()
	{
		System.out.println("***--- Number bug records "+bugs.size()+" Number file records "+files.size()+"---***\n");
		
		for(int i=0;i<bugs.size();i++)
		{
			//bugs.get(i).printData();
		}
	}
	
	   @SuppressWarnings("finally")
		public int rows_number(String tableName) throws ApplicationException 
	   {
	       String query = "SELECT Count(*) as total FROM " + tableName;
	       int rows_number = 0 ;
	       try 
	       {
	           connection = ConnectionFactory.getConnection();
	           statement = connection.createStatement();
	           resultSet =statement.executeQuery(query);
	           
	           while(resultSet.next())
	           {
	           	rows_number = resultSet.getInt("total");
	           }
	           
	           SQLWarning warning = statement.getWarnings();
	           if (warning != null)
	               throw new ApplicationException(warning.getMessage());
	       } 
	       catch (SQLException e) 
	       {
	           ApplicationException exception = new ApplicationException(e.getMessage(), e);
	           throw exception;
	       } 
	       finally 
	       {
	           Tools.close(statement);
	           Tools.close(connection);
	           Tools.close(resultSet);
	           System.out.println("Query Successfully Executed, size of "+tableName+" is "+rows_number);
	           return rows_number;
	       }
	   }

}
