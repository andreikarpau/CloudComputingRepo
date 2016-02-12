package capstone.task2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.storage.StorageLevel;

import capstone.task2.FlightInformation;
import capstone.task2.FlightInformation.ColumnNames;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class G1T1RankAirports {
	public static void main(String[] args) {
		if (args.length < 4) {
			System.exit(1);
		}

		Map<String, String> paramsMap = new HashMap<String, String>();
		SparkConf sparkConf = new SparkConf().setAppName("G1T1RankAirports");
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		
		MapReduceHelper.fillBaseStreamingParams(args, paramsMap, sparkConf, topicMap);
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, paramsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2());

		JavaDStream<String> lines = messages.map(MapReduceHelper.getBaseInputPreprocessingFunction());

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
	
		JavaPairDStream<String, Integer> fullRDD = sums.transformToPair(MapReduceHelper.getRDDJoinWithPreviousFunction());	
		JavaPairDStream<String, Integer> sorted = fullRDD.transformToPair(MapReduceHelper.<String, Integer>getSortFunction());

		sorted.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
	}
}
