package capstone.task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class DataProducer {
    private static final Logger LOG = Logger.getLogger(DataProducer.class);
    private final String TOPIC = "flightsTopic";
	private Producer<String, String> producer;
	
	public static void main(String[] args) throws IOException {
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
		
		DataProducer dataProducer = new DataProducer(config);
		dataProducer.ProduceFromFile(inputFile);
	}
	
	public DataProducer(ProducerConfig config)
	{
		producer = new Producer<String, String>(config);
	}
	
	public void ProduceFromFile(String inputFile) throws IOException{
		LOG.debug("Reading and Sending Data");

		BufferedReader bufferReader = new BufferedReader(new FileReader(inputFile));

		try {
			String line;	

			while ((line = bufferReader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty())
					continue;
				
				ProduceFile(line);		
			}			
		} finally {
			bufferReader.close();
		}
		
		LOG.debug("Closing Producer");
		producer.close();
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

				KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, line);
				producer.send(data);
			}			
		} finally {
			bufferReader.close();
		}
		LOG.debug("End Processing file:" + fileName);		
	}
}
