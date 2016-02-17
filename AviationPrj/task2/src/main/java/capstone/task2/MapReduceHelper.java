package capstone.task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import capstone.task2.FlightInformation.ColumnNames;

public class MapReduceHelper {
	public static final String INPUT_PARAMS = "inputParams";
	public static final String TOPIC = "flightsTopic1";
	public static final String FLUSH_RDD_FLAG = "flush.rdd.needed";
	public static Boolean flushRDD = false;
	public static Long recordsCount = 0l;
	public static final String SEPARATOR = "_";
	
	private static final Integer WAIT_TIME = 40000;
	private MapReduceHelper() {}
	
	public static void awaitTermination(JavaStreamingContext jssc){
		while (true){
			Long oldCount = recordsCount;
			
			if (jssc.awaitTerminationOrTimeout(WAIT_TIME)){
				System.out.println("\n -------------Stream was stopped");
				return;
			}else{
					jssc.awaitTerminationOrTimeout(WAIT_TIME);
					
				    if (recordsCount.equals(oldCount)){
						System.out.println("\n -------------Flush results records operated = "  + recordsCount);
						flushRDD = true;
						jssc.awaitTerminationOrTimeout(WAIT_TIME);
						return;
				    }	
			}
			
			System.out.println("\n -------------RECORDS COUNT = " + recordsCount);
		}
	}
	
	public static void fillBaseStreamingParams(String[] args, Map<String, String> paramsMap, SparkConf sparkConf, Map<String, Integer> topicMap, String folderName) throws IOException{
		paramsMap.put("auto.offset.reset", "smallest");
		paramsMap.put("zookeeper.connect", args[0]);
		paramsMap.put("metadata.broker.list", args[0].split(":")[0] + ":6667");
		paramsMap.put("group.id", args[1]);

		//paramsMap.put("rebalance.backoff.ms", "20000");
		paramsMap.put("zookeeper.connection.timeout.ms", "20000");

		sparkConf.set("spark.connection.cassandra.host", args[2]).set("spark.cassandra.connection.port", args[3]);
		sparkConf.set(FLUSH_RDD_FLAG, "false");
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "3000000");
		
		int numThreads = 1;

		if (5 <= args.length) {
			numThreads = Integer.parseInt(args[4]);
		}

		String topicName = MapReduceHelper.TOPIC;
		if (6 <= args.length) {
			topicName = args[5];
		}

		if (7 <= args.length) {
			String fileName = args[6];
			String cachedStr = "";
			
			BufferedReader bufferReader = new BufferedReader(new FileReader(fileName));

			try {
				String line;

				while ((line = bufferReader.readLine()) != null) {
					if (line.isEmpty())
						continue;

					cachedStr = cachedStr + ";" + line;
				}
			} finally {
				bufferReader.close();
				sparkConf.set(INPUT_PARAMS, cachedStr);
			}
		}
		
		topicMap.put(topicName, numThreads);
	}
	
	public static Function<Tuple2<String, String>, String> getBaseInputPreprocessingFunction(){
		return new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String, String> arg0) throws Exception {
				recordsCount++;
				return arg0._2();
			}
		};
	}

	public static String CompareFlights(String v1, String v2) throws Exception {
		if (v1 == null)
			return v2;
		
		if (v2 == null)
			return v1;
			
		String[] values1 = v1.split(SEPARATOR);				
		String[] values2 = v2.split(SEPARATOR);				
		Integer arrTime1 = Integer.parseInt(values1[3].toString());
		Integer arrTime2 = Integer.parseInt(values2[3].toString());
		
		if (arrTime1 < arrTime2)
		{
			return v1;
		}
		else
		{
			return v2;
		}
	}
		
	public static <A, B> PairFlatMapFunction<Tuple2<A, B>, B, A> getRDDFlipFunction(){
		return new PairFlatMapFunction<Tuple2<A, B>, B, A>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<B, A>> call(Tuple2<A, B> pair) throws Exception {
				ArrayList<Tuple2<B, A>> list = new ArrayList<Tuple2<B, A>>();
				list.add(new Tuple2<B, A>(pair._2(), pair._1()));
				return list;
			}
		};
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
	
	public static Function<Tuple2<String,Tuple2<Long,Integer>>, Boolean> GetFilterEmpty(){
		return new Function<Tuple2<String,Tuple2<Long,Integer>>, Boolean>(){
			private static final long serialVersionUID = 1L;
			public Boolean call(Tuple2<String, Tuple2<Long, Integer>> pair) throws Exception {
				if (pair == null || pair._1() == null || pair._1().isEmpty() || pair._2() == null)
					return false;
			
				return true;
			}
		};
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
}

