package capstone.task2;

import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

public class CassandraHelper {

	private static final String KEYSPACE = "keyspacecapstone";
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CassandraHelper.class);
    private Cluster cluster;
    private Session session;
    private PreparedStatement preparedStatement;

    public Session getSession()  {
         LOG.info("Starting getSession()");
        if (this.session == null && (this.cluster == null || this.cluster.isClosed())) {
            LOG.info("Cluster not started or closed");
        } else if (this.session.isClosed()) {
            LOG.info("session is closed. Creating a session");
            this.session = this.cluster.connect();
        }

        return this.session;
    }

    public void createConnection(String node, Integer port)  {

        this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();

        Metadata metadata = cluster.getMetadata();
            
        System.out.printf("Connected to cluster: %s\n",metadata.getClusterName());
        
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();

        
        //this.prepareQueries();

    }

    public void closeConnection() {
        cluster.close();
    }

    public static String createCreateTableScript(String tableName, String[] columnsNames){
    	String finalScript = "CREATE TABLE " + KEYSPACE + ".";
        
    	finalScript += tableName +" (";
    	
    	for (int i = 0; i < columnsNames.length; i++) {
    		finalScript += columnsNames[i] + " varchar";
    		
    		if (i < (columnsNames.length - 1))
    		{
    			finalScript += ", ";
    		}
    		else
    		{
    			finalScript += ", PRIMARY KEY (" + columnsNames[0] + "))";
    		}
		}  
    	finalScript += ";";
    	return finalScript;
    }
    
    public void createTable(String tableName, String[] columnNames){
    	String finalScript = createCreateTableScript(tableName, columnNames);
    	System.out.printf("Create table script = " + finalScript);
    	
    	session.execute(finalScript);
    }
    
    public static String createAddValuesScript(String tableName, String[] columnNames){
    	String finalScript = "INSERT INTO " + KEYSPACE + ".";
    	finalScript = finalScript + tableName +" (";
    	String questionMarks = "VALUES (";
    	
    	for (int i = 0; i < columnNames.length; i++) {
    		finalScript = finalScript + columnNames[i];
    		questionMarks = questionMarks + "?";
    		
    		if (i < (columnNames.length - 1))
    		{
    			finalScript = finalScript + ", ";
    			questionMarks = questionMarks + ", ";
    		}
    	}
    	
    	questionMarks = questionMarks + ")";
    	finalScript = finalScript + ") " + questionMarks + ";";
    	
    	return finalScript;
    }
    
    public void addValues(String tableName, String[] columnNames, Object[] values){    	 
    	String finalScript = createAddValuesScript(tableName, columnNames);
    	System.out.printf("Add values script = " + finalScript);
    	
    	session.execute(finalScript, values);
    }
    
    public void prepareQueries(String script)  {
    	System.out.printf("PreparedStatement script = " + script);
        this.preparedStatement = this.session.prepare(script);
    }

    public void addKey(Object[] values) {
        Session session = this.getSession();
        
        if (0 < values.length) {
            try {
                session.execute(this.preparedStatement.bind(values) );
                //session.executeAsync(this.preparedStatement.bind(key));
            } catch (NoHostAvailableException e) {
                System.out.printf("No host in the %s cluster can be contacted to execute the query.\n", 
                        session.getCluster());
                Session.State st = session.getState();
                for ( Host host : st.getConnectedHosts() ) {
                    System.out.println("In flight queries::"+st.getInFlightQueries(host));
                    System.out.println("open connections::"+st.getOpenConnections(host));
                }

            } catch (QueryExecutionException e) {
                System.out.println("An exception was thrown by Cassandra because it cannot " +
                        "successfully execute the query with the specified consistency level.");
            }  catch (IllegalStateException e) {
                System.out.println("The BoundStatement is not ready.");
            }
        }
    }
}