package ucb.oseproxy.server;

import java.sql.*;
import java.util.HashMap;
import java.util.logging.Logger;

import ucb.oseproxy.rpc.ProxyServer;
import ucb.oseproxy.util.ProxyUtil;

public class OSEServer {
  static final String JDBC_DRIVER = "org.postgresql.Driver";
  static final String DB_URL = "jdbc:postgresql://%s:%d/EMP";
  private static OSEServer instance = null;
  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());
  private HashMap<String, Connection> connMap;

  public static OSEServer getInstance() {
    if (instance == null)
      instance = new OSEServer();
    return instance;
  }

  protected OSEServer() {
    connMap = new HashMap<String,Connection>();
    // STEP 2: Register JDBC driver
    try {
      Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      logger.warning("JDBC Driver initialization failed");
    }

  }

  public String connectClient(String clientID, String host, int port, String username, String password) {
    String db_url = String.format(DB_URL, host, port);
    // Always get a fresh connection until we can figure out how to get client info
    /*
    if (connMap.get(clientId + db_url) != null) {
      return db_url.hashCode();
    } else {
    */
    try {
      // STEP 3: Open a connection
      logger.info("Connecting to database" + host + ":" + port);
      Connection conn = DriverManager.getConnection(DB_URL, username, password);
      String uuid = ProxyUtil.randomId();
      connMap.put(uuid, conn);
      return uuid;
    } catch (SQLException se) {
      logger.warning("SQL exception" + se.getMessage());
      return null;
    }
  }

  public Connection getConnection(int hash) {
    if (connMap != null) {
      return connMap.get(new Integer(hash));
    } else
      return null;
  }

}

