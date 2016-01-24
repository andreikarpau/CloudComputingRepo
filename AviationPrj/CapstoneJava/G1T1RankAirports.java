package capstoneHadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class G1T1RankAirports extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new G1T1RankAirports(), args);
		System.exit(res);
	}
	
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/Capstone/Tmp/TopAirports");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airports Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportsCountMap.class);
        jobA.setReducerClass(AirportsCountReduce.class);

        String filesStr = readHDFSFile(args[0], conf);
        String[] files = filesStr.split("\n");
        
        for (int i = 0; i < files.length; i++) {
			String inputName = files[i];
			
			if (inputName != null && !inputName.trim().isEmpty())
				FileInputFormat.addInputPath(jobA, new Path(inputName.trim()));
		}
        
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        
        jobA.setJarByClass(G1T1RankAirports.class);
        jobA.waitForCompletion(true);
        
        Job jobB = Job.getInstance(conf, "Airports Count");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirportsMap.class);
        jobB.setReducerClass(TopAirportsReduce.class);
        jobB.setNumReduceTasks(1);

        Path outputPath = new Path("/Capstone/Output/TopAirports");
        fs.delete(outputPath, true);
        
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, outputPath);

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(G1T1RankAirports.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
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
 
    public static class AirportsCountMap extends Mapper<Object, Text, Text, IntWritable> {
    	ColumnNames[] columns = new ColumnNames[] { ColumnNames.Origin, ColumnNames.Dest };

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || value.toString().trim().isEmpty())
            	return;
            
            FlightInformation information = new FlightInformation(value.toString().trim(), columns);
            
			String origin = information.GetValues()[0];
			String dest = information.GetValues()[1];
			
			if (!origin.isEmpty())
				context.write(new Text(origin), new IntWritable(1));
			
			if (!dest.isEmpty())
				context.write(new Text(dest), new IntWritable(1));
        }
    }

    public static class AirportsCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	for (IntWritable val : values) {
        		sum += val.get();
        	}
        	context.write(key, new IntWritable(sum));
        }
    }
    
    public static class TopAirportsMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N = 10;
        private TreeSet<Pair<Integer, String>> countToAirportsMap = new TreeSet<Pair<Integer, String>>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer count = Integer.parseInt(value.toString());
        	String airportName = key.toString();
        	
        	countToAirportsMap.add(new Pair<Integer, String>(count, airportName));
        	
        	if (countToAirportsMap.size() > N) {
        		countToAirportsMap.remove(countToAirportsMap.first());
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Integer, String> item : countToAirportsMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	}
        }
    }

    public static class TopAirportsReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N = 10;
        private TreeSet<Pair<Integer, String>> countToAirportsMap = new TreeSet<Pair<Integer, String>>();

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (TextArrayWritable val: values) {
        		Text[] pair= (Text[]) val.toArray();
        		String airport = pair[0].toString();
        		Integer count = Integer.parseInt(pair[1].toString());
        		countToAirportsMap.add(new Pair<Integer, String>(count, airport));

        		if (countToAirportsMap.size() > N) {
        			countToAirportsMap.remove(countToAirportsMap.first());
        		}
        	}
        	
        	Iterator<Pair<Integer, String>> iterator = countToAirportsMap.descendingIterator();
        	
        	while(iterator.hasNext())
            {
        		Pair<Integer, String> item = iterator.next();
        		Text airport = new Text(item.second);
        		IntWritable value = new IntWritable(item.first);
        		context.write(airport, value);
            }
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