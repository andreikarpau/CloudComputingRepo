package capstone2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Tuple2;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class G1T1RankAirports {
	 private static final Pattern COMMA = Pattern.compile(",");

	  private G1T1RankAirports() {
	  }

	  public static void main(String[] args) {
	    if (args.length < 2) {
	      System.exit(1);
	    }

	    StreamingExamples.setStreamingLogLevels();
	    
	    SparkConf sparkConf = new SparkConf().setAppName("G1T1RankAirports");

	    // Create the context with 2 seconds batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

	    int numThreads = 1;
	    
	    if (2 <= args.length) {
	    	numThreads = Integer.parseInt(args[2]);
	    }
	    
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] topics = args[2].split(",");
	    
	    for (String topic: topics) {
	      topicMap.put(topic, numThreads);
	    }

	    JavaPairReceiverInputDStream<String, String> messages =
	            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	      @Override
	      public String call(Tuple2<String, String> tuple2) {
	        return tuple2._2();
	      }
	    });
/*
	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      @Override
	      public Iterable<String> call(String x) {
	        return (Iterable<String>) Arrays.asList(SPACE.split(x)).iterator();
	      }
	    });

	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, 1);
	        }
	      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      });
*/
	    lines.print();
	    jssc.start();
	    jssc.awaitTermination();
	  }
}

