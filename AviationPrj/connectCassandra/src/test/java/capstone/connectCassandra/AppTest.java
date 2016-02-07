package capstone.connectCassandra;

import java.io.IOException;
import java.util.ArrayList;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     * @throws IOException 
     */
    public void testApp() throws IOException
    {
    	TestCreateTableFromFile();   
    	TestInsertValuesFromFile();
    }
    
    private void TestInsertValuesFromFile() throws IOException
    {
    	ArrayList<String[]> list = InputReaderHelper.GetRowsToAdd("testInput.txt");
    	
    	assertTrue(list.get(0)[0].equals("2008-12-01"));
    	assertTrue(list.get(0)[1].equals("DCU"));
    	assertTrue(list.get(0)[2].equals("ATL"));
    	assertTrue(list.get(0)[3].equals("1201"));
    	assertTrue(list.get(0)[4].equals("0112"));

    	assertTrue(list.get(1)[0].equals("2008-10-11"));
    	assertTrue(list.get(1)[1].equals("UCD"));
    	assertTrue(list.get(1)[2].equals("LTA"));
    	assertTrue(list.get(1)[3].equals("1011"));
    	assertTrue(list.get(1)[4].equals("1110"));
    	
    	String[] columns = InputReaderHelper.GetColumnsToAdd("test.txt");
    	String script = CassandraHelper.createAddValuesScript("bestTravel", columns);
    	assertTrue(script.equals("INSERT INTO keyspacecapstone.bestTravel (date1, origin1, destination1, arrTime1, flightId1) VALUES (?, ?, ?, ?, ?);"));
    }
    
    private void TestCreateTableFromFile() throws IOException
    {
    	String[] columns = InputReaderHelper.GetColumnsToAdd("test.txt");
    	assertTrue(columns.length == 5);
    	assertTrue(columns[0].equals("date1"));
    	assertTrue(columns[1].equals("origin1"));
    	assertTrue(columns[2].equals("destination1"));
    	assertTrue(columns[3].equals("arrTime1"));
    	assertTrue(columns[4].equals("flightId1"));
    
    	String script = CassandraHelper.createCreateTableScript("bestTravel", columns);
    	assertTrue(script.equals("CREATE TABLE keyspacecapstone.bestTravel (date1 varchar, origin1 varchar, destination1 varchar, arrTime1 varchar, flightId1 varchar, PRIMARY KEY (date1));"));
    }
}
