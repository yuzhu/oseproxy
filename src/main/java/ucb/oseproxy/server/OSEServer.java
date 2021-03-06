package ucb.oseproxy.server;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import ucb.oseproxy.rpc.ProxyServer;
import ucb.oseproxy.smo.SMOCommand;
import ucb.oseproxy.smo.SMOCommand.Command;
import ucb.oseproxy.smo.SMOFactory;
import ucb.oseproxy.util.ProxyUtil;

public class OSEServer {
  static final String JDBC_DRIVER = "org.postgresql.Driver";
  static final String DB_URL = "jdbc:postgresql://%s:%d/%s";
  private static OSEServer instance = null;
  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());
  // map a connID to connection
  private ConcurrentHashMap<String, Connection> connMap;
  // map a resultSetId to a ResultSet
  private HashMap<String, ResultSet> rsMap;
  // map a db to a SMO
  private HashMap<String, SMOCommand> smoMap; 
  
  private HashMap<String, Stack<SMOCommand>> smoStackMap;

  public synchronized static OSEServer getInstance() {
    if (instance == null)
      instance = new OSEServer();
    return instance;
  }

  protected OSEServer() {
    connMap = new ConcurrentHashMap<String, Connection>();
    rsMap = new HashMap<String, ResultSet>();
    smoMap = new HashMap<String, SMOCommand>();
    smoStackMap = new HashMap<String, Stack<SMOCommand>>();
    // STEP 2: Register JDBC driver
    try {
      Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      logger.warning("JDBC Driver initialization failed");
    }

  }

  public synchronized String connectClient(String clientID, String host, int port, String dbname, String username,
      String password) {
    String db_url = String.format(DB_URL, host, port, dbname);
    try {
      // STEP 3: Open a connection
      logger.info("Connecting to database"  + db_url);
      Connection conn = DriverManager.getConnection(db_url, username, password);
      String uuid = ProxyUtil.randomId();
      if (conn == null ) logger.warning("Drivemanager returned null connection");
      //logger.info("putting in connMap" + uuid);
      
      connMap.put(uuid, conn);
      //logger.info(connMap.toString());
      Stack<SMOCommand> newstack = new Stack<SMOCommand>();
      smoStackMap.put(uuid, newstack);
      
      // Perform initialization of the connection for SMOs
      // Statement stmt = conn.createStatement();
      //stmt.executeUpdate("CREATE LANGUAGE plperl;");
      
      // TODO: Solve concurrent update issue 
      // stmt.executeUpdate("CREATE OR REPLACE FUNCTION set_var(name text, val text) RETURNS text AS $$ if ($_SHARED{$_[0]} = $_[1]) {return 'ok';} else { return \"can't set shared variable $_[0] to $_[1]\";}$$ LANGUAGE plperl;");
      // stmt.executeUpdate("CREATE OR REPLACE FUNCTION get_var(name text) RETURNS text AS $$return $_SHARED{$_[0]};$$ LANGUAGE plperl;");
      logger.info("connection string" + uuid);
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
    //logger.info("trying to get connection " +connId);
    Connection conn = this.getConnection(connId);
    if (conn == null) {
      logger.warning("connection null for " + connId);
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
  public String execSMOString(String connId, String smoString) {
    // find the db the conn belong to
    Connection conn = this.getConnection(connId);
    try {
      String dbURL = conn.getMetaData().getURL();
      logger.info("execSMO dbURL" + dbURL);
      String uuid = ProxyUtil.randomId();
      SMOCommand smo = SMOFactory.getSMO(smoString);
      smo.connect(conn);
      
      Stack<SMOCommand> stack = this.smoStackMap.get(connId);
      stack.push(smo);
      
      smoMap.put(uuid, smo);
      smo.executeSMO(); 
      // XXX: Auto-Commit 
      smo.commitSMO();
      return uuid;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  public boolean commitStack(String connId) {
    Stack<SMOCommand> stack = this.smoStackMap.get(connId);
    for(SMOCommand cmd : stack) {
      cmd.commitSMO();
    }
    return true;
  }
  
  public boolean rollbackStack(String connId) {
    boolean result = true;
    Stack<SMOCommand> stack = this.smoStackMap.get(connId);
    while (!stack.isEmpty()) {
      SMOCommand cmd = stack.pop();
      if (!cmd.rollbackSMO())
        result = false;
    }
    return result;
  }
  
  public String execSMO(String connId, Command cmd, List<String> options ) {
    // find the db the conn belong to
    Connection conn = this.getConnection(connId);
    try {
      String dbURL = conn.getMetaData().getURL();
      logger.info("execSMO dbURL" + dbURL);
      String uuid = ProxyUtil.randomId();
      SMOCommand smo = SMOFactory.getSMO(conn, cmd, options);
      
      Stack<SMOCommand> stack = this.smoStackMap.get(connId);
      stack.push(smo);
      
      smoMap.put(uuid, smo);
      smo.executeSMO(); 
      return uuid;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
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
      logger.info(connMap.toString());
      return connMap.get(connId);
    } else
    {
      logger.warning("connMap is null");
      return null;
    }  
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

