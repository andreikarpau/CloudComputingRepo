package capstone.hadoopMVN;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapReduceHelper {
	private MapReduceHelper() {}

	public static String readHDFSFile(String path, Configuration conf) throws IOException{
	    Path pt=new Path(path);
	    FileSystem fs = FileSystem.get(pt.toUri(), conf);
	    FSDataInputStream file = fs.open(pt);
	    BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));
	
	    StringBuilder everything = new StringBuilder();
	    String line;
	    while( (line = buffIn.readLine()) != null) {
	        everything.append(line);
	        everything.append("\n");
	    }
	    return everything.toString();
	}
	
	public static FlightInformation[] readValuesFromFile(BufferedReader buffIn, ColumnNames[] names) throws IOException{
	    ArrayList<FlightInformation> list = new ArrayList<FlightInformation>();
	    String line;
	    while((line = buffIn.readLine()) != null) {
	    	if (line.trim().length() <= 0) 
	    	{
	    		continue;
	    	}
	    	
	    	list.add(new FlightInformation(line, names));
	    }	

	    FlightInformation[] result = new FlightInformation[list.size()];
	    return list.toArray(result);
	}

	public static FlightInformation[] readValuesFromFile(String path, Configuration conf, ColumnNames[] names) throws IOException{
	    Path pt=new Path(path);
	    FileSystem fs = FileSystem.get(pt.toUri(), conf);
	    FSDataInputStream file = fs.open(pt);
	    BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));
	
	    return readValuesFromFile(buffIn, names);
	}
}

