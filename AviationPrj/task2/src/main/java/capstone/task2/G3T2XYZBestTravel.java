package capstone.task2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import capstone.task2.FlightInformation.ColumnNames;
import capstone.task2.MapReduceHelper.SummingToUse;

public class G3T2XYZBestTravel {
	public static void main(String[] args) throws IOException {
		if (args.length < 4) {
			System.exit(1);
		}

		String className = G3T2XYZBestTravel.class.getSimpleName();
		Map<String, String> paramsMap = new HashMap<String, String>();
		SparkConf sparkConf = new SparkConf().setAppName(className);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		
		MapReduceHelper.fillBaseStreamingParams(args, paramsMap, sparkConf, topicMap, className);
		final String filterStr = sparkConf.get(MapReduceHelper.INPUT_PARAMS);
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(8000));
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, paramsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2());

		JavaDStream<String> lines = messages.map(MapReduceHelper.getBaseInputPreprocessingFunction());
		JavaPairDStream<String, String> flights = lines.flatMapToPair(GetMapWithInputFunction(filterStr)).
				reduceByKey(GetFilterBestFlights());
	
		
		JavaPairDStream<String, String> fullRDD = flights.transformToPair(MapReduceHelper.<String, String>getRDDJoinWithPreviousFunction(SummingToUse.CompareFlightsAggregating));			
		JavaPairDStream<String, String> sorted = fullRDD.transformToPair(G3T2XYZBestTravel.getSortFunction());
		
		sorted.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
	}

	public static Function2<String, String, String> GetFilterBestFlights(){
		return new Function2<String, String, String>(){
			private static final long serialVersionUID = 1L;
			public String call(String v1, String v2) throws Exception {
				return MapReduceHelper.CompareFlights(v1, v2);
			}
		};
	}
	
	public static PairFlatMapFunction<String, String, String> GetMapWithInputFunction(final String filterStr){
		return new PairFlatMapFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;
			ColumnNames[] columns = new ColumnNames[] { ColumnNames.Origin, ColumnNames.Dest, ColumnNames.DepTime, ColumnNames.ArrTime, ColumnNames.FlightDate, ColumnNames.FlightId };
	    	private ArrayList<Information> OriginDestList = null;
	    	
	    	private ArrayList<Information> fillOriginDestList() throws IOException{		    		
	    		ArrayList<Information> originDestList = new ArrayList<Information>();
	    		
	    		String[] airports = filterStr.split(";");
				DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

				for (int i = 0; i < airports.length; i++) {
					String string = airports[i].trim();
													
					// Get rid of redundant spaces
					String[] array = string.split("\\s+");
					
					if (array.length < 4)
						continue;
					
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
					
					originDestList.add(new Information(path1, i, 0, format.format(flightDate1).trim()));
					originDestList.add(new Information(path2, i, 1, format.format(flightDate2).trim()));
				}
				
				return originDestList;
	    	}
	    		    	
			public Iterable<Tuple2<String, String>> call(String value) throws Exception {
				ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
				
				if (value == null || value.toString().trim().isEmpty())
	            	return list;	       
				
				if (OriginDestList == null)
					OriginDestList = fillOriginDestList();
				                       
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
					return list;

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
					
					String keyNum = listInfo.ItemNum.toString() + MapReduceHelper.SEPARATOR + listInfo.FlightNum;
					String val = listInfo.FlightDate + MapReduceHelper.SEPARATOR + origin + MapReduceHelper.SEPARATOR + 
							dest + MapReduceHelper.SEPARATOR + arrTime + MapReduceHelper.SEPARATOR + flightId + 
							MapReduceHelper.SEPARATOR + listInfo.FlightNum;
					
					list.add(new Tuple2<String, String>(keyNum, val));
				}

				return list;
			}
		};
	}
	
	public static Function<JavaPairRDD<String,String>, JavaPairRDD<String, String>> getSortFunction(){
		return new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
			private static final long serialVersionUID = 1L;
			private Boolean isWritten = false;
						
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> pairs) throws Exception {
				JavaPairRDD<String, String> rdd = pairs.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<String, String> call(Tuple2<String, String> pair) throws Exception {
						String flightNum = pair._1().split(MapReduceHelper.SEPARATOR)[0];
						return new Tuple2<String, String>(flightNum, pair._2());
					}			 
				 }).reduceByKey(new Function2<String, String, String>() {
						private static final long serialVersionUID = 1L;
						public String call(String val1, String val2) throws Exception {
							if (val1 == null || val1.isEmpty() || val2 == null || val1.isEmpty())
								return "";
							
							Integer val1FlightNum = Integer.parseInt(val1.split(MapReduceHelper.SEPARATOR)[5]);
							Integer val2FlightNum = Integer.parseInt(val2.split(MapReduceHelper.SEPARATOR)[5]);
							
							if (val1FlightNum < val2FlightNum)
							{
								return val1 + MapReduceHelper.SEPARATOR + val2;
							}

							return val2 + MapReduceHelper.SEPARATOR + val1;
						}
					});
				
				if (!isWritten && MapReduceHelper.flushRDD)
				{
					isWritten = true;
					System.out.println("\n-------WRITE TO CASSANDRA 1------ ");

					String cassandraIp = rdd.context().getConf().get("spark.connection.cassandra.host");
					Integer cassandraPort = Integer.parseInt(rdd.context().getConf().get("spark.cassandra.connection.port"));

					CassandraHelper cassandraHelper = new CassandraHelper();
					cassandraHelper.createConnection(cassandraIp, cassandraPort);
					
					List<Tuple2<String, String>> list = rdd.toArray();
					cassandraHelper.prepareQueries("INSERT INTO keyspacecapstone.findflight (Date1, Origin1, Dest1, Time1, FlightId1, Date2, Origin2, Dest2, Time2, FlightId2) VALUES (?,?,?,?,?,?,?,?,?,?);");

                	Object[] values = new Object[10];
                	
                    for (Tuple2<String, String> tuple2 : list) {             
                    	if (tuple2._2().equals(""))
                    		continue;
                    	
                    	String[] tripInfo = tuple2._2().split(MapReduceHelper.SEPARATOR);
                    	                 	
                    	values[0] = tripInfo[0];
                    	values[1] = tripInfo[1];
                    	values[2] = tripInfo[2];
                    	values[3] = tripInfo[3];
                    	values[4] = tripInfo[4];
                    	values[5] = tripInfo[6];
                    	values[6] = tripInfo[7];
                    	values[7] = tripInfo[8];
                    	values[8] = tripInfo[9];
                    	values[9] = tripInfo[10];
                    	
                    	System.out.println("\n--------CASSANDRA " + tuple2._1() + " " + tuple2._2());
                    	
                    	cassandraHelper.addKey(values);
                    	Thread.sleep(100);
					}
                    
    				cassandraHelper.closeConnection();
				}
				
				return rdd;
			}
		};
	}
	
	public static class Information
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
}