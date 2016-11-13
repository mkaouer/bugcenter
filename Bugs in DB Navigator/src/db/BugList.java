package db;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


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
		System.out.println("***--- Number bug records "+bugs.size()+" Number file records "+files.size()+"---***");
		
		for(int i=0;i<2;i++)
		{
			bugs.get(i).printData();
			//files.get(i).printData();
		}
		for(int i=0;i<2;i++)
		{
			//bugs.get(i).printData();
			files.get(i).printData();
		}
	}
	
	void assignBugsToFiles()
	{
		for (int i = 0; i < files.size(); i++) 
		{
			files.get(i).split_bug_ids(this.bugs);
		}
	}
	
	void export_files_data()
    {
        Date dNow = new Date( );
        SimpleDateFormat ft = 
        new SimpleDateFormat ("yyyy.MM.dd'-'hh.mm.ss");
        
        String file_name = new String("./output/buggy_files_dates");
        file_name = file_name.concat(ft.format(dNow));
        file_name = file_name.concat(".csv");
        
        try
	{
	    FileWriter writer = new FileWriter(file_name);
	    writer.append("file_name,  bug_id, bug_year\n");   
            
	    for(int i=0;i<files.size();i++)
            {
                writer.append(files.get(i).file_name+","+files.get(i).bug_id+","
                		+files.get(i).bug_date);
                writer.append('\n');
            }
	    //generate whatever data you want
 
	    writer.flush();
	    writer.close();
	}
        catch(IOException e)
        {
        	e.printStackTrace();
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
