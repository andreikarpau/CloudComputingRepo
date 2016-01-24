package capstone.hadoopMVN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import capstone.hadoopMVN.ColumnNames;
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
     */
    public void testApp()
    {
    	TestFilesParse();
    }
    
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
    		assertTrue(infos[1].GetValues()[1].equals("NA"));
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
