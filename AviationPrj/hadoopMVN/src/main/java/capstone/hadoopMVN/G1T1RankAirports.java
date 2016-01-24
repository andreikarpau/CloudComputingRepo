package capstone.hadoopMVN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class G1T1RankAirports extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/Capstone/Output/" + Text.class.getSimpleName());
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airports Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportsCountMap.class);
        jobA.setReducerClass(AirportsCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(G1T1RankAirports.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
        
/*        Job jobB = Job.getInstance(conf, "Top Titles");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopTitlesMap.class);
        jobB.setReducerClass(TopTitlesReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopTitles.class);
        return jobB.waitForCompletion(true) ? 0 : 1;*/
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
    	Configuration confing;
    	ColumnNames[] columns = new ColumnNames[] { ColumnNames.Origin, ColumnNames.Dest };
    	
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
        	confing = context.getConfiguration();
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = value.toString();
            FlightInformation[] informations = MapReduceHelper.readValuesFromFile(filePath, confing, columns);
            
            for (int i = 0; i < informations.length; i++) {
				FlightInformation flightInformation = informations[i];
				
				String origin = flightInformation.GetValues()[0];
				String dest = flightInformation.GetValues()[1];
				
				if (!origin.isEmpty())
					context.write(new Text(origin), new IntWritable(1));
				
				if (!dest.isEmpty())
					context.write(new Text(dest), new IntWritable(1));
			}
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
}

