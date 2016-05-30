package ucb.oseproxy.server;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import ucb.oseproxy.rpc.ProxyServer;
import ucb.oseproxy.smo.SMOCommand;
import ucb.oseproxy.smo.SMOCommand.Command;
import ucb.oseproxy.util.ProxyUtil;

public class OSEServer {
  static final String JDBC_DRIVER = "org.postgresql.Driver";
  static final String DB_URL = "jdbc:postgresql://%s:%d/%s";
  private static OSEServer instance = null;
  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());
  private HashMap<String, Connection> connMap;
  private HashMap<String, ResultSet> rsMap;
  // map a db to a SMO
  private HashMap<String, SMOCommand> smoMap; 

  public static OSEServer getInstance() {
    if (instance == null)
      instance = new OSEServer();
    return instance;
  }

  protected OSEServer() {
    connMap = new HashMap<String, Connection>();
    rsMap = new HashMap<String, ResultSet>();
    smoMap = new HashMap<String, SMOCommand>();
    // STEP 2: Register JDBC driver
    try {
      Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      logger.warning("JDBC Driver initialization failed");
    }

  }

  public String connectClient(String clientID, String host, int port, String dbname, String username,
      String password) {
    String db_url = String.format(DB_URL, host, port, dbname);
    // Always get a fresh connection until we can figure out how to get client info
    /*
     * if (connMap.get(clientId + db_url) != null) { return db_url.hashCode(); } else {
     */
    try {
      // STEP 3: Open a connection
      logger.info("Connecting to database"  + db_url);
      Connection conn = DriverManager.getConnection(db_url, username, password);
      String uuid = ProxyUtil.randomId();
      connMap.put(uuid, conn);
      return uuid;
    } catch (SQLException se) {
      logger.warning("SQL exception" + se.getMessage());
      return null;
    }
  }

  public int execUpdate(String connId, String query) {
    Connection conn = this.getConnection(connId);
    if (conn == null) {
      return 0;
    }
    Statement stmt;
    int retval;
    try {
      stmt = conn.createStatement();
      retval = stmt.executeUpdate(query);
    } catch (SQLException e) {
      logger.warning("SQL exception for connId" + connId + ": " + e.toString());
      return 0;
    }  
    return retval;
  }
  
  public String execQuery(String connId, String query) {
    Connection conn = this.getConnection(connId);
    if (conn == null) {
      return null;
    }
    Statement stmt;
    ResultSet rs = null;
    try {
      stmt = conn.createStatement();
      rs = stmt.executeQuery(query);
    } catch (SQLException e) {
      logger.warning("SQL exception for connId" + connId + ": " + e.toString());
      return null;
    }  
    if (rs != null) {
      String uuid = ProxyUtil.randomId();
      rsMap.put(uuid, rs);
      return uuid;
    }

    return null;
  }
  
  public void execSMO(String connId, Command cmd, List<String> options ) {
    // find the db the conn belong to
    Connection conn = this.getConnection(connId);
    try {
      String dbURL = conn.getMetaData().getURL();
      logger.info("execSMO dbURL" + dbURL);
      SMOCommand smo = new SMOCommand(conn, cmd, options);
      smoMap.put(dbURL, smo);
      smo.createView();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // find out if there are any outstanding SMOs
    
  }

  public ResultSet getResultSet(String rsId) {
    if (rsId != null)
      return rsMap.get(rsId);
    else return null;
  }

  public Connection getConnection(String connId) {
    if (connMap != null) {
      return connMap.get(connId);
    } else
      return null;
  }

  public Map<String, Object> getNextRow(String resultSetId) {
    ResultSet rs = this.getResultSet(resultSetId);
    Map<String, Object> row = new HashMap<String, Object>();
    try {
      if (!rs.next()) return null;
      for (int i=0; i<rs.getMetaData().getColumnCount(); i++) {
        row.put(rs.getMetaData().getColumnName(i+1), rs.getObject(i+1));
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
    return row;

  }


}

