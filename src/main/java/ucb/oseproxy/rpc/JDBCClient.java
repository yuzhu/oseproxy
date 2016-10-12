package ucb.oseproxy.rpc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class JDBCClient {
  static final String JDBC_DRIVER = "org.postgresql.Driver";
  static final String DB_URL = "jdbc:postgresql://%s:%d/%s";
  private static final Logger logger = Logger.getLogger(JDBCClient.class.getName());
  public static void main(String[] args) throws SQLException {
    
    String dbURL = "";
    int dbPort = 5432;
    String dbname = "";
    String username = "";
    String password = "";
    if (args.length != 5) {
      logger.info("Using some default values for parameters");
    } else {
      dbURL = args[0];
      dbPort = Integer.parseInt(args[1]);
      dbname = args[2];
      username = args[3];
      password = args[4];
    }
    
    
    
    String db_url = String.format(DB_URL, dbURL, dbPort, dbname);
    Connection conn = DriverManager.getConnection(db_url, username, password); 
    Statement stmt = conn.createStatement();
    int querycount = 0;
    long start_time = System.nanoTime();
    for ( int i=1; i < 2000; i++) {
     ResultSet rs = stmt.executeQuery("select * from persons where personid="+ i);
     rs.next();
     if (i%100 == 0) logger.info("i is " + i);
     querycount++;
    }
    long end_time = System.nanoTime();
    double qps = querycount/ ((end_time - start_time)/1e9);
    System.out.println("Query per second" + qps);

  }

}
