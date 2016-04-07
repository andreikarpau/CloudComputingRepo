package capstone.task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class DataProducer {
	private ProducerConfig producerConfig;
	private static String TopicName = MapReduceHelper.TOPIC;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length < 3) {

			System.out.println("No input file");
			System.exit(-1);
		}

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);
		props.put("zk.connect", args[1]);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");	
		
		ProducerConfig config = new ProducerConfig(props);

		System.out.println("\n Creating Producer");
		String inputFile = args[2];
		
		if (4 <= args.length)
			TopicName = args[3];
		
		DataProducer dataProducer = new DataProducer(config);
		dataProducer.ProduceFromFile(inputFile);
	}
	
	public DataProducer(ProducerConfig config)
	{
		producerConfig = config;
	}
	
	public void ProduceFromFile(String inputFile) throws IOException, InterruptedException{
		System.out.println("\n Reading and Sending Data");

		BufferedReader bufferReader = new BufferedReader(new FileReader(inputFile));
		ExecutorService executor = Executors.newFixedThreadPool(8);
		
		try {
			String line;	

			while ((line = bufferReader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty())
					continue;
				
				Runnable worker = new ProduceFileThread(line, producerConfig);	
				executor.execute(worker);
			}			
		} finally {
			bufferReader.close();
		}
		
        executor.shutdown();
        while (!executor.isTerminated()) {
        	Thread.sleep(50000);
        }
		
		System.out.println("\n Closing Producer");
	}
	
	private static class ProduceFileThread implements Runnable {
		private String inputFile;
		private ProducerConfig producerConfig;
		
		public ProduceFileThread(String fileName, ProducerConfig config){
			inputFile = fileName;
			producerConfig = config;
		}
		
		public void run() {
			try {
				ProduceFile(inputFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		private void ProduceFile(String fileName) throws IOException
		{
			Producer<String, String> producer = new Producer<String, String>(producerConfig);

			BufferedReader bufferReader = new BufferedReader(new FileReader(fileName));
			System.out.println("\n Start Processing file:" + fileName);

			try {
				String line;	

				while ((line = bufferReader.readLine()) != null) {
					if (line.isEmpty())
						continue;

					KeyedMessage<String, String> data = new KeyedMessage<String, String>(TopicName, line);
					producer.send(data);
				}			
			} finally {
				bufferReader.close();
				producer.close();
			}
			System.out.println("\n End Processing file:" + fileName);
		}
	}
}
