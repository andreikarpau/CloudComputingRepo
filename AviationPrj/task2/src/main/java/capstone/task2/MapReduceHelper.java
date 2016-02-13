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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import capstone.task2.FlightInformation.ColumnNames;

import com.google.common.base.Optional;

public class MapReduceHelper {
	public static enum SummingToUse 
	{ 
		G1T1, 
		G1T3
	}
	
	public static final String INPUT_PARAMS = "inputParams";
	public static final String TOPIC = "flightsTopic1";
	public static final String FLUSH_RDD_FLAG = "flush.rdd.needed";
	public static Boolean flushRDD = false;
	
	public static SummingToUse summingToUse;
	
	private MapReduceHelper() {}
	
	public static void fillBaseStreamingParams(String[] args, Map<String, String> paramsMap, SparkConf sparkConf, Map<String, Integer> topicMap, String folderName) throws IOException{
		paramsMap.put("auto.offset.reset", "smallest");
		paramsMap.put("zookeeper.connect", args[0]);
		paramsMap.put("group.id", args[1]);
		paramsMap.put("zookeeper.connection.timeout.ms", "10000");

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

	public static <A, B> B sumPairValues(Tuple2<A, Tuple2<Optional<B>, Optional<B>>> pair) throws Exception {
		if (summingToUse == SummingToUse.G1T1)
		{
			Integer value = 0;
			if (pair._2()._1().isPresent())
				value += (Integer)pair._2()._1().get();

			if (pair._2()._2().isPresent())
				value += (Integer)pair._2()._2().get();
			
			return (B)value;
		}
			
		if (summingToUse == SummingToUse.G1T3)
		{
			Tuple2<Long, Integer> value = new Tuple2<Long, Integer>(0L, 0);
			if (pair._2()._1().isPresent())
			{
				Tuple2<Long, Integer> newVal = (Tuple2<Long, Integer>)pair._2()._1().get();
				value = new Tuple2<Long, Integer>(newVal._1() + value._1(), newVal._2() + value._2());
			}

			if (pair._2()._2().isPresent())
			{
				Tuple2<Long, Integer> newVal = (Tuple2<Long, Integer>)pair._2()._2().get();
				value = new Tuple2<Long, Integer>(newVal._1() + value._1(), newVal._2() + value._2());
			}	
			
			return (B)value;
		}
		
		throw new Exception("Correct sumPairValues function is not set!");
		//return null;
	}
	
	public static <A, B> Function<JavaPairRDD<A, B>, JavaPairRDD<A, B>> getRDDJoinWithPreviousFunction(){
		return new Function<JavaPairRDD<A, B>, JavaPairRDD<A, B>>() {
			private static final long serialVersionUID = 1L;
			JavaPairRDD<A, B> prevRdd = null;
			
			public JavaPairRDD<A, B> call(JavaPairRDD<A, B> rdd) throws Exception {
				JavaPairRDD<A, B> newRdd = rdd;		

				if (prevRdd != null) {
					newRdd = prevRdd;
					
					if (!rdd.take(1).isEmpty())
					{
						newRdd = rdd.fullOuterJoin(prevRdd).flatMapToPair(new PairFlatMapFunction<Tuple2<A, Tuple2<Optional<B>, Optional<B>>>, A, B>() {
							private static final long serialVersionUID = 1L;
	
							public Iterable<Tuple2<A, B>> call(Tuple2<A, Tuple2<Optional<B>, Optional<B>>> pair) throws Exception {
								B value = sumPairValues(pair);
								ArrayList<Tuple2<A, B>> list = new ArrayList<Tuple2<A, B>>();
								list.add(new Tuple2<A, B>(pair._1(), value));
								return list;
							}
						});
					}
					
					if (rdd.take(1).isEmpty() && !newRdd.take(1).isEmpty()){
						flushRDD = true;
					}
				}

				prevRdd = newRdd;
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

