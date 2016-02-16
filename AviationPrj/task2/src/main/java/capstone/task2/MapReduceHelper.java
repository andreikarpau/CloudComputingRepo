package capstone.task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import capstone.task2.FlightInformation.ColumnNames;

public class MapReduceHelper {
	public static enum SummingToUse 
	{ 
		IntegerSumming, 
		LongIntTuplesSumming,
		CompareFlightsAggregating
	}
	
	public static final String INPUT_PARAMS = "inputParams";
	public static final String TOPIC = "flightsTopic1";
	public static final String FLUSH_RDD_FLAG = "flush.rdd.needed";
	public static Boolean flushRDD = false;
	public static final String SEPARATOR = "_";
	
	private MapReduceHelper() {}
	
	public static void fillBaseStreamingParams(String[] args, Map<String, String> paramsMap, SparkConf sparkConf, Map<String, Integer> topicMap, String folderName) throws IOException{
		paramsMap.put("auto.offset.reset", "smallest");
		paramsMap.put("zookeeper.connect", args[0]);
		paramsMap.put("group.id", args[1]);

		paramsMap.put("rebalance.backoff.ms", "20000");
		paramsMap.put("zookeeper.connection.timeout.ms", "40000");

		sparkConf.set("spark.connection.cassandra.host", args[2]).set("spark.cassandra.connection.port", args[3]);
		sparkConf.set(FLUSH_RDD_FLAG, "false");
		
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
				return arg0._2();
			}
		};
	}

	public static <B> B sumPairValues(B v1, B v2, SummingToUse summingToUse) throws Exception {
		if (summingToUse == SummingToUse.IntegerSumming) {
			Integer value = 0;
				value += (Integer)v1;
				value += (Integer)v2;
			
			return (B)value;
		}
			
		if (summingToUse == SummingToUse.LongIntTuplesSumming) {
			Tuple2<Long, Integer> value = new Tuple2<Long, Integer>(0L, 0);

			Tuple2<Long, Integer> newVal1 = (Tuple2<Long, Integer>)v1;
			value = new Tuple2<Long, Integer>(newVal1._1() + value._1(), newVal1._2() + value._2());

			Tuple2<Long, Integer> newVal2 = (Tuple2<Long, Integer>)v2;
			value = new Tuple2<Long, Integer>(newVal2._1() + value._1(), newVal2._2() + value._2());

			return (B)value;
		}
		
		if (summingToUse == SummingToUse.CompareFlightsAggregating){

			return (B)CompareFlights((String)v1, (String)v2);
		}
		
		throw new Exception("Correct sumPairValues function is not set!");
		//return null;
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
	
	public static <A, B> Function<JavaPairRDD<A, B>, JavaPairRDD<A, B>> getRDDJoinWithPreviousFunction(final SummingToUse summingToUse){
		return new Function<JavaPairRDD<A, B>, JavaPairRDD<A, B>>() {
			private static final long serialVersionUID = 1L;
			List<Tuple2<A, B>> prevRdd = null;
			private int counter = 0;
			
			public JavaPairRDD<A, B> call(JavaPairRDD<A, B> rdd) throws Exception {
				JavaPairRDD<A, B> newRdd = rdd;		

				if (prevRdd != null) {				
					/*newRdd = rdd.flatMapToPair(new PairFlatMapFunction<Tuple2<A,B>, A, B>() {

						public Iterable<Tuple2<A, B>> call(Tuple2<A, B> t) throws Exception {
							return prevRdd;
						}
						
					});
					
					if (!rdd.take(1).isEmpty())
					{
						newRdd = rdd.union(prevRdd).reduceByKey(new Function2<B, B, B>() {
							private static final long serialVersionUID = 1L;
							public B call(B v1, B v2) throws Exception {
								B value = sumPairValues(v1, v2, summingToUse);
								return value;
							}
						});
					}*/
					
					if (rdd.take(1).isEmpty() && !newRdd.take(1).isEmpty()){
						counter++;
						if (3 < counter)
							flushRDD = true;
					}else{
						counter = 0;
					}
				}

				prevRdd = newRdd.toArray();
				return newRdd;
			}
		};
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
    /*
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
		public int compareTo(MapReduceHelper.Pair<A, B> o) {
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
	}*/
}

