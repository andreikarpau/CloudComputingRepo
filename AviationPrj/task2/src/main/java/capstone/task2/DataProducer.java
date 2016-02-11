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

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class DataProducer {
    private static final Logger LOG = Logger.getLogger(DataProducer.class);
	private Producer<String, String> producer;
	private static String TopicName = MapReduceHelper.TOPIC;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length < 3) {

			System.out.println("No input file");
			System.exit(-1);
		}

		ConsoleAppender console = new ConsoleAppender(); //create appender
		String PATTERN = "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n";
		console.setLayout(new PatternLayout(PATTERN)); 
		console.setThreshold(Level.DEBUG);
		console.activateOptions();
		LOG.addAppender(console);
		
		LOG.debug("Creating Properties");

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);
		props.put("zk.connect", args[1]);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");	
		
		ProducerConfig config = new ProducerConfig(props);

		LOG.debug("Creating Producer");
		String inputFile = args[2];
		
		if (4 <= args.length)
			TopicName = args[4];
		
		DataProducer dataProducer = new DataProducer(config);
		dataProducer.ProduceFromFile(inputFile);
	}
	
	public DataProducer(ProducerConfig config)
	{
		producer = new Producer<String, String>(config);
	}
	
	public void ProduceFromFile(String inputFile) throws IOException, InterruptedException{
		LOG.debug("Reading and Sending Data");

		BufferedReader bufferReader = new BufferedReader(new FileReader(inputFile));
		ExecutorService executor = Executors.newFixedThreadPool(10);
		
		try {
			String line;	

			while ((line = bufferReader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty())
					continue;
				
				Runnable worker = new ProduceFileThread(line, producer);	
				executor.execute(worker);
			}			
		} finally {
			bufferReader.close();
		}
		
        executor.shutdown();
        while (!executor.isTerminated()) {
        	Thread.sleep(10000);
        }
		
		LOG.debug("Closing Producer");
		producer.close();
	}
	
	private static class ProduceFileThread implements Runnable {
		private String inputFile;
		Producer<String, String> producer;
	    
		public ProduceFileThread(String fileName, Producer<String, String> newProducer){
			inputFile = fileName;
			producer = newProducer;
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
			BufferedReader bufferReader = new BufferedReader(new FileReader(fileName));
			LOG.debug("Start Processing file:" + fileName);				

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
			}
			LOG.debug("End Processing file:" + fileName);		
		}
	}
}
