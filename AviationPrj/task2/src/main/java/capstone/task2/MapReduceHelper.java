package capstone.task2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
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

import com.google.common.base.Optional;

import capstone.task2.FlightInformation.ColumnNames;

public class MapReduceHelper {
	public static final String TOPIC = "flightsTopic1";
	public static final String FLUSH_RDD_FLAG = "flush.rdd.needed";
	
	private MapReduceHelper() {}
	
	public static void fillBaseStreamingParams(String[] args, Map<String, String> paramsMap, SparkConf sparkConf, Map<String, Integer> topicMap){
		paramsMap = new HashMap<String, String>();
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
	
	public static <A, B> Function<JavaPairRDD<A, B>, JavaPairRDD<A, B>> getSortFunction(){
		return new Function<JavaPairRDD<A, B>, JavaPairRDD<A, B>>() {
			private static final long serialVersionUID = 1L;

			public JavaPairRDD<A, B> call(JavaPairRDD<A, B> pairs) throws Exception {
				return pairs.flatMapToPair(MapReduceHelper.<A, B>getRDDFlipFunction()).
				sortByKey(false).flatMapToPair(MapReduceHelper.<B, A>getRDDFlipFunction());
			}
		};
	}

	public static Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>> getRDDJoinWithPreviousFunction(){
		return new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
			private static final long serialVersionUID = 1L;
			JavaPairRDD<String, Integer> prevRdd = null;
			
			public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> rdd) throws Exception {
				JavaPairRDD<String, Integer> newRdd = rdd;		

				if (prevRdd != null) {
					newRdd = rdd.fullOuterJoin(prevRdd).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>>, String, Integer>() {
						private static final long serialVersionUID = 1L;

						public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>> pair) throws Exception {
									Integer value = 0;

									if (pair._2()._1().isPresent())
										value += pair._2()._1().get();

									if (pair._2()._2().isPresent())
										value += pair._2()._2().get();

									ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
									list.add(new Tuple2<String, Integer>(pair._1(), value));
									return list;
						}
					});
					
					if (rdd.take(1).isEmpty() && !newRdd.take(1).isEmpty()){
						rdd.context().getConf().set(FLUSH_RDD_FLAG, "true");
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

