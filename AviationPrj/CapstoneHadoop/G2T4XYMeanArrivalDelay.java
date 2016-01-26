package capstoneHadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class G2T4XYMeanArrivalDelay extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new G2T4XYMeanArrivalDelay(), args);
		System.exit(res);
	}
	
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("destFileName", args[1]);
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("/Capstone/Output/MeanXYDelay");
        fs.delete(outputPath, true);

        Job jobA = Job.getInstance(conf, "Arr Delay Mean");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(DoubleWritable.class);

        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntWritable.class);
        
        jobA.setMapperClass(ArrDelayCountMap.class);
        jobA.setReducerClass(ArrDelayCountReduce.class);

        String filesStr = readHDFSFile(args[0], conf);
        String[] files = filesStr.split("\n");
        
        for (int i = 0; i < files.length; i++) {
			String inputName = files[i];
			
			if (inputName != null && !inputName.trim().isEmpty())
				FileInputFormat.addInputPath(jobA, new Path(inputName.trim()));
		}
        
        FileOutputFormat.setOutputPath(jobA, outputPath);
        jobA.setOutputFormatClass(TextOutputFormat.class);
        jobA.setJarByClass(G2T4XYMeanArrivalDelay.class);
        
        return jobA.waitForCompletion(true) ? 0 : 1;
    }
 
    public static class ArrDelayCountMap extends Mapper<Object, Text, Text, IntWritable> {
    	ColumnNames[] columns = new ColumnNames[] { ColumnNames.Origin, ColumnNames.Dest, ColumnNames.ArrDelayMinutes };

    	List<String> airports;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			String destFileName = conf.get("destFileName");
			String[] items = readHDFSFile(destFileName, conf).split("\n");
			
			for (int i = 0; i < items.length; i++) {
				String string = items[i].trim();
				
				// Get rid of redundant spaces
				String[] array = string.split("\\s+");
				items[i] = array[0] + " " + array[1];
			}
			
			airports = Arrays.asList(items);
		}
    	
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || value.toString().trim().isEmpty())
            	return;
            
            FlightInformation information = new FlightInformation(value.toString().trim(), columns);
            
			String origin = information.GetValues()[0];		
			String dest = information.GetValues()[1];	
			String orgingDest = origin + " " + dest;
			
			if (origin.isEmpty() || dest.isEmpty() || !airports.contains(orgingDest))
				return;

			String delayMinutes = information.GetValue(ColumnNames.ArrDelayMinutes);		
			
			if (delayMinutes.isEmpty())
				return;
			
			Integer delayMinutesVal = Integer.parseInt(delayMinutes);
			context.write(new Text(orgingDest), new IntWritable(delayMinutesVal));
        }
    }

    public static class ArrDelayCountReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {    	
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	int valuesCount = 0;
        	
        	for (IntWritable val : values) {
        		sum += val.get();
        		valuesCount++;
        	}
        	
        	double delaySum = sum;
        	double mean = 0;
        	
        	if (valuesCount != 0)
        		mean = delaySum / valuesCount;
        	
        	context.write(key, new DoubleWritable(mean));
        }
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
    
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
    
    enum ColumnNames 
    { 
    	Origin(0), 
    	Dest(1), 
    	AirlineID(2), 
    	AirlineCode(3), 
    	ArrDelayed(4), 
    	DepDelayed(5), 
    	ArrDelayMinutes(6), 
    	FlightDate(7), 
    	DayOfWeek(8), 
    	CRSDepatureTime(9), 
    	ArrTime(10);
    	
        private final int value;

        ColumnNames(final int newValue) {
            value = newValue;
        }

        public int getValue() { return value; }
    }

    public static class FlightInformation 
    {
    	private static final int COLUMNS_COUNT = 11;
    	
    	String[] values;
    	ColumnNames[] keys;
    	
    	public FlightInformation(String informationRowLine, ColumnNames[] columnNames)
    	{
    		String[] parts = informationRowLine.split(",");
    		
    		if (parts.length < COLUMNS_COUNT)
    		{
    			String[] allColumns = new String[COLUMNS_COUNT];
    		
    			for (int i = 0; i < allColumns.length; i++) {
    				if (i < parts.length && parts[i] != null) {
    					allColumns[i] = parts[i].trim();
    				} else {
    					allColumns[i] = "";
    				}
    			}
    			
    			parts = allColumns;
    		}
    		
    		values = new String[columnNames.length];
    		keys = new ColumnNames[columnNames.length];

    		for (int i = 0; i < columnNames.length; i++)
    		{
    			values[i] = parts[columnNames[i].getValue()].trim();
    			
    			if (values[i].equals("NA"))
    				values[i] = "";
    			
    			keys[i] = columnNames[i];
    		}
    		
    	}
    	
    	public String[] GetValues()
    	{
    		return values;
    	}
    	
    	public String GetValue(ColumnNames columnName)
    	{
    		for (int i = 0; i < keys.length; i++)
    		{
    			if (keys[i] == columnName)
    			{
    				return values[i];
    			}
    		}
    		
    		return "";
    	}
    	
    	public ColumnNames[] GetKeys()
    	{
    		return keys;
    	}
    }

	public static class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>> implements Comparable<Pair<A, B>> {
		
		public final A first;
		public final B second;
	
		public Pair(A first, B second) {
			this.first = first;
			this.second = second;
		}
	
		public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
				A first, B second) {
			return new Pair<A, B>(first, second);
		}
	
		@Override
		public int compareTo(Pair<A, B> o) {
			int cmp = o == null ? 1 : (this.first).compareTo(o.first);
			return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
		}
	
		@Override
		public int hashCode() {
			return 31 * hashcode(first) + hashcode(second);
		}
	
		private static int hashcode(Object o) {
			return o == null ? 0 : o.hashCode();
		}
	
		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Pair))
				return false;
			if (this == obj)
				return true;
			return equal(first, ((Pair<?, ?>) obj).first)
					&& equal(second, ((Pair<?, ?>) obj).second);
		}
	
		private boolean equal(Object o1, Object o2) {
			return o1 == o2 || (o1 != null && o1.equals(o2));
		}
	
		@Override
		public String toString() {
			return "(" + first + ", " + second + ')';
		}
	}
}