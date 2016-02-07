package capstone.connectCassandra;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	if (args.length < 5)
    		return;
    	
    	String ip = args[0].trim();
    	Integer port = Integer.parseInt(args[1].trim());

    	String tableName = args[2].trim();
    	String columnsFile = args[3].trim();
    	String inputFile = args[4].trim();

    	String[] columns = InputReaderHelper.GetColumnsToAdd(columnsFile);
    	ArrayList<String[]> values = InputReaderHelper.GetRowsToAdd(inputFile);
    	
    	CassandraHelper cassandraHelper = new CassandraHelper();
    	cassandraHelper.createConnection(ip, port);
    	cassandraHelper.createTable(tableName, columns);
    	
    	for (String[] input : values) {
    		cassandraHelper.addValues(tableName, columns, input);
		}
    	
    	cassandraHelper.closeConnection();
    }
}
