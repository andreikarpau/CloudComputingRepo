package capstone.hadoopMVN;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import capstone.hadoopMVN.FlightInformation.ColumnNames;
import capstone.hadoopMVN.MapReduceHelper.Pair;
import capstone.hadoopMVN.MapReduceHelper.TextArrayWritable;

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

        String filesStr = MapReduceHelper.readHDFSFile(args[0], conf);
        String[] files = filesStr.split("\n");
        
        for (int i = 0; i < files.length; i++) {
			String inputName = files[i];
			
			if (inputName != null && !inputName.trim().isEmpty())
				FileInputFormat.addInputPath(jobA, new Path(inputName.trim()));
		}
        
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        
        jobA.setJarByClass(G1T1RankAirports.class);
        jobA.waitForCompletion(true);
        
        Job jobB = Job.getInstance(conf, "Rank Airports");
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
}

