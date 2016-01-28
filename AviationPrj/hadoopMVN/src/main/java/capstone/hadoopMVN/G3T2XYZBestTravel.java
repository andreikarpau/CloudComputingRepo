package capstone.hadoopMVN;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import capstone.hadoopMVN.FlightInformation.ColumnNames;
import capstone.hadoopMVN.MapReduceHelper.TextArrayWritable;

public class G3T2XYZBestTravel extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new G3T2XYZBestTravel(), args);
		System.exit(res);
	}
	
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("destFileName", args[1]);
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("/Capstone/Output/FindFlight");
        fs.delete(outputPath, true);

        Job jobA = Job.getInstance(conf, "Arr Delay Mean");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(TextArrayWritable.class);
        
        jobA.setMapperClass(FindFlightsMap.class);
        jobA.setReducerClass(FindFlightsReduce.class);

        String filesStr = MapReduceHelper.readHDFSFile(args[0], conf);
        String[] files = filesStr.split("\n");
        
        for (int i = 0; i < files.length; i++) {
			String inputName = files[i];
			
			if (inputName != null && !inputName.trim().isEmpty())
				FileInputFormat.addInputPath(jobA, new Path(inputName.trim()));
		}
        
        FileOutputFormat.setOutputPath(jobA, outputPath);
        jobA.setOutputFormatClass(TextOutputFormat.class);
        jobA.setJarByClass(G3T2XYZBestTravel.class);
        
        return jobA.waitForCompletion(true) ? 0 : 1;
    }
 
    public static class FindFlightsMap extends Mapper<Object, Text, Text, TextArrayWritable> {
    	ColumnNames[] columns = new ColumnNames[] { ColumnNames.Origin, ColumnNames.Dest, ColumnNames.DepTime, ColumnNames.ArrTime, ColumnNames.FlightDate, ColumnNames.FlightId };

    	ArrayList<Information> OriginDestList = new ArrayList<Information>();

    	private static class Information
    	{
    		public String OriginDest;
    		public Integer ItemNum;
    		public Integer FlightNum;
    		public String FlightDate;
    		
    		public Information(String originDest, Integer itemNum, Integer flightNum, String flightDate)
    		{
    			OriginDest = originDest;
    			ItemNum = itemNum;
    			FlightNum = flightNum;
    			FlightDate = flightDate.toString();
    		}
    	}
    	
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			String destFileName = conf.get("destFileName");
			String[] items = MapReduceHelper.readHDFSFile(destFileName, conf).split("\n");
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

			for (int i = 0; i < items.length; i++) {
				String string = items[i].trim();
												
				// Get rid of redundant spaces
				String[] array = string.split("\\s+");
				
				if (array.length < 4)
					return;
				
				String path1 = array[0] + " " + array[1];
				String path2 = array[1] + " " + array[2];
				
				Date flightDate1;
				try {
					flightDate1 = format.parse(array[3]);
				} catch (ParseException e) {
					e.printStackTrace();
					throw new IOException();
				}
				
				Calendar calendar = Calendar.getInstance(); 
				calendar.setTime(flightDate1);
				calendar.add(Calendar.DATE, 2);
				Date flightDate2 = calendar.getTime();
				
				OriginDestList.add(new Information(path1, i, 0, format.format(flightDate1).trim()));
				OriginDestList.add(new Information(path2, i, 1, format.format(flightDate2).trim()));
			}
		}
    	
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || value.toString().trim().isEmpty())
            	return;
            
            FlightInformation information = new FlightInformation(value.toString().trim(), columns);
            
			String origin = information.GetValues()[0].trim();		
			String dest = information.GetValues()[1].trim();	
			String orgingDest = origin + " " + dest;

			String depTime = information.GetValue(ColumnNames.DepTime);		
			String arrTime = information.GetValue(ColumnNames.ArrTime);	
			String flightId = information.GetValue(ColumnNames.FlightId);		
			String flightDate = information.GetValue(ColumnNames.FlightDate);	

			if (origin.isEmpty() || dest.isEmpty() || origin.isEmpty() || dest.isEmpty() || depTime.isEmpty() || arrTime.isEmpty() ||
					flightId.isEmpty() || flightDate.isEmpty())
				return;

			Integer depTimeVal = Integer.parseInt(depTime);
			
			for (Information listInfo : OriginDestList) {
				if (!listInfo.OriginDest.equals(orgingDest) || !listInfo.FlightDate.equals(flightDate))
					continue;

				if (listInfo.FlightNum == 0)
				{
					if (1200 < depTimeVal)
					{
						continue;
					}
				}
				else
				{
					if (depTimeVal < 1200)
					{
						continue;
					}
				}
				
				String keyNum = listInfo.ItemNum.toString();
				TextArrayWritable val = new TextArrayWritable(new String[] { listInfo.FlightNum.toString(), arrTime, flightId, orgingDest, listInfo.FlightDate });
				context.write(new Text(keyNum), val);
			}
        }
    }

    public static class FindFlightsReduce extends Reducer<Text, TextArrayWritable, Text, Text> { 
    	
        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	Integer leastArrTime1 = 9999;
        	Integer leastArrTime2 = 9999;
        	
        	Text[] info1 = null;
        	Text[] info2 = null;
        	
        	for (TextArrayWritable val: values) {
        		Text[] info = (Text[]) val.toArray();
        		String flightNum = info[0].toString();
    			Integer arrTime = Integer.parseInt(info[1].toString());

        		if (flightNum.equals("0"))
        		{        			
        			if (arrTime < leastArrTime1)
        			{
        				leastArrTime1 = arrTime;
        				info1 = info;
        			}
        		}
        		else
        		{
        			if (arrTime < leastArrTime2)
        			{
        				leastArrTime2 = arrTime;
        				info2 = info;
        			}
        		}
        	}
        	
        	if (9999 <= leastArrTime1 || 9999 <= leastArrTime2)
        		return;
        	
        	String flightId1 = info1[2].toString();
        	String originDest1 = info1[3].toString();
        	String date1 = info1[4].toString();
        	
        	String flightId2 = info2[2].toString();
        	String originDest2 = info2[3].toString();
        	String date2 = info2[4].toString();
        	
        	context.write(new Text(date1 + " " + originDest1 + " " + leastArrTime1.toString() + " " + flightId1 + " "),
        			new Text(date2 + " " + originDest2 + " " + leastArrTime2.toString() + " " + flightId2));
        }
    }
}