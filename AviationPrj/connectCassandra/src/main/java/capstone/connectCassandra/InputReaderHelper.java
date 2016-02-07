package capstone.connectCassandra;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class InputReaderHelper {
	
	public static String[] GetColumnsToAdd(String fileName) throws IOException{
		BufferedReader bufferReader = new BufferedReader(new FileReader(fileName));
		String line;
		ArrayList<String> list = new ArrayList<String>();
		
		while ((line = bufferReader.readLine()) != null) {
			if (line.trim().isEmpty())
				continue;
			
			list.add(line.trim());
		}
		
		bufferReader.close();
		return list.toArray(new String[list.size()]);
	}

	public static ArrayList<String[]> GetRowsToAdd(String fileName) throws IOException{
		BufferedReader bufferReader = new BufferedReader(new FileReader(fileName));
		String line;
		ArrayList<String[]> list = new ArrayList<String[]>();
		
		while ((line = bufferReader.readLine()) != null) {
			line = line.trim();
			
			if (line.isEmpty())
				continue;
	
			String[] values = line.split("\\s+");
			if (values == null || values.length <= 0)
				continue;
			
			for (int i = 0; i < values.length; i++) {
				values[i] = values[i].replace(":", "");
			}
			
			list.add(values);
		}
		
		bufferReader.close();
		return list;
	}
}
