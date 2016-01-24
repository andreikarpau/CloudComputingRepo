package capstone.hadoopMVN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import capstone.hadoopMVN.G1T1RankAirports;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
    	if (args == null || args.length <= 0)
    	{
            System.exit(0);
    		return;
    	}
    	
    	String[] argsHadoop = new String[args.length];   	
    	for (int i = 0; i < args.length; i++) {
    		if (i == 0)
    			continue;
    		
    		argsHadoop[i - 1] = args[i];
		}
    	
    	int res = 0;
    	
    	switch(args[0])
    	{
    		case "RankAirports":
    	        res = ToolRunner.run(new Configuration(), new G1T1RankAirports(), argsHadoop);
    			break;
    	}
    	
        System.exit(res);
    }
}
