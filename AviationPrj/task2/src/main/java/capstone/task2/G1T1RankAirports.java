package capstone.task2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import capstone.task2.FlightInformation;
import capstone.task2.FlightInformation.ColumnNames;
import scala.Tuple2;

public class G1T1RankAirports {
	public static void main(String[] args) {
		if (args.length < 4) {
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("G1T1RankAirports")
				.set("spark.connection.cassandra.host", args[2])
				.set("spark.cassandra.connection.port", args[3]);

		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				new Duration(5000));

		int numThreads = 1;

		if (5 <= args.length) {
			numThreads = Integer.parseInt(args[4]);
		}

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(MapReduceHelper.TOPIC, numThreads);
//		topicMap.put("auto.offset.reset", "smallest");
//		KafkaUtils.createStream(jssc, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String, String> arg0) throws Exception {
				return arg0._2();
			}
		});

		JavaPairDStream<String, Integer> sums = lines.flatMapToPair(
				new PairFlatMapFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;
					ColumnNames[] columns = new ColumnNames[] { ColumnNames.Origin, ColumnNames.Dest };
		
					public Iterable<Tuple2<String, Integer>> call(String value) throws Exception {
						ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
		
						if (value == null || value.toString().trim().isEmpty())
							return list;
		
						FlightInformation information = new FlightInformation(
								value.toString().trim(), columns);
		
						String origin = information.GetValues()[0];
						if (!origin.isEmpty())
							list.add(new Tuple2<String, Integer>(origin, 1));
		
						String dest = information.GetValues()[1];
						if (!dest.isEmpty())
							list.add(new Tuple2<String, Integer>(dest, 1));
		
						return list;
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						private static final long serialVersionUID = 1L;
			
						public Integer call(Integer value1, Integer value2)
								throws Exception {
							return value1 + value2;
						}
		});
	
		Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>> transform = MapReduceHelper.GetRDDJoinFunction();
		
		JavaPairDStream<String, Integer> fullRDD = sums.transformToPair(transform);	
		JavaPairDStream<String, Integer> sorted = fullRDD.transformToPair(MapReduceHelper.<String, Integer>GetSortAndFilter10Function());
		
		sorted.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
	}
}