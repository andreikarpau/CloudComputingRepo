package capstone.hadoopMVN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import capstone.hadoopMVN.FlightInformation.ColumnNames;
import capstone.hadoopMVN.FlightInformation;
import capstone.hadoopMVN.MapReduceHelper;
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
     * @throws ParseException 
     */
    public void testApp() throws ParseException
    {
    	TestFilesParse();
    	
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		
		String string = "2006-07-03";
		Date flightDate1 = format.parse(string);
		
		Calendar calendar = Calendar.getInstance(); 
		calendar.setTime(flightDate1);
		calendar.add(Calendar.DATE, 2);
		Date flightDate2 = calendar.getTime();

		String fl1 = format.format(flightDate1).trim();
		String fl2 = format.format(flightDate2).trim();
		
		assertTrue(fl1.equals(string));
		assertTrue(fl2.equals("2006-07-05"));	
    }
/*    
    private static void TestCassandra()
    {
    	String host = "54.210.238.87";
    	Integer port = 9042;
        CassandraHelper client = new CassandraHelper();
        
        //Create the connection
        client.createConnection(host, port);
        System.out.println("starting writes");
        
        //Add test value
        client.addKey("test1234");
        
        //Close the connection
        client.closeConnection();
        
        System.out.println("Write Complete");
    }
*/    
    private static void TestFilesParse()
    {
    	try 
    	{
    		String fileName = "TestSmall.csv";
    		BufferedReader br = new BufferedReader(new FileReader(fileName));
    		FlightInformation[] infos = MapReduceHelper.readValuesFromFile(br, 
    				new ColumnNames[] {ColumnNames.Origin, ColumnNames.ArrDelayMinutes, ColumnNames.ArrTime});
    	
    		assertTrue(infos.length == 3);
    		
    		assertTrue(infos[0].GetValue(ColumnNames.Origin).equals("BDL"));
    		assertTrue(infos[0].GetValue(ColumnNames.ArrDelayMinutes).equals("4"));
    		assertTrue(infos[0].GetValue(ColumnNames.ArrTime).equals("1724"));

    		assertTrue(infos[1].GetValues()[0].equals(""));
    		assertTrue(infos[1].GetValues()[1].equals(""));
    		assertTrue(infos[1].GetValues()[2].equals(""));

    		assertTrue(infos[2].GetValues()[0].equals("BDL"));
    		assertTrue(infos[2].GetValues()[1].equals("0"));
    		assertTrue(infos[2].GetValues()[2].equals("2215"));
    	}
    	catch (IOException ex)
    	{
    		assertTrue(false);
    	}
    }
}
