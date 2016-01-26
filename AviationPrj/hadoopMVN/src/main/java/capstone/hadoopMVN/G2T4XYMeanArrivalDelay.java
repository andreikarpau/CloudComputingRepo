package capstone.hadoopMVN;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

        String filesStr = MapReduceHelper.readHDFSFile(args[0], conf);
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
			String[] items = MapReduceHelper.readHDFSFile(destFileName, conf).split("\n");
			
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
}