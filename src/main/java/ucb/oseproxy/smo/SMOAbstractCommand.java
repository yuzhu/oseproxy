package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import ucb.oseproxy.smo.SMOCommand.Command;

public abstract class SMOAbstractCommand implements SMOCommand {
  protected Connection conn;
  protected Command cmd;
  protected List<String> args;
  protected List<String> resultTables;
  protected List<String> views = new ArrayList<String>();
  protected List<String> tables = new ArrayList<String>();
  
  private static Logger logger = Logger.getLogger(SMOAbstractCommand.class.getName());
  
  public SMOAbstractCommand(){};
  
  public SMOAbstractCommand(Connection conn, Command cmd2, List<String> options) {
    // make a clone of the connection
    try {
      String dbURL = conn.getMetaData().getURL();
      logger.info("dburl" + dbURL);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    this.conn = conn;
    this.cmd = cmd2;
    this.args = options;
    resultTables = new ArrayList<String>();
  }
  
  private String antiLoopVar(String tablename, String operation) {
    return "triggered " + tablename + operation;
  }
  protected String setupTriggerFuncforView(String tablename, String operation, String funcBody) throws SQLException{
    Statement stmt = conn.createStatement();
    StringBuilder triggerString = new StringBuilder();
    String antiLoop = antiLoopVar(tablename, operation);
    String triggerFunc = tablename + "_" + operation + "_func";
    triggerString.append("CREATE OR REPLACE FUNCTION "+ triggerFunc );
    triggerString.append("() RETURNS trigger AS $" + triggerFunc + "$");
    triggerString.append(" BEGIN ");
    //triggerString.append(" IF get_var('"+ antiLoop + "') = 'no' THEN PERFORM set_var('"+ antiLoop +"', 'yes');");
    triggerString.append(funcBody);
    //triggerString.append( "PERFORM set_var('"+ antiLoop +"', 'no'); END IF;");
    triggerString.append(" RETURN NEW; ");
    triggerString.append("END; ");
    triggerString.append("$" + triggerFunc + "$ LANGUAGE plpgsql;");
    logger.info(triggerString.toString());
    stmt.executeUpdate(triggerString.toString());
    return triggerFunc;
  }
  

  protected void attachTrigger(Statement stmt, String tablename, String triggerName,  String triggerFunc, String op) throws SQLException {
    
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TRIGGER ");
    sb.append(triggerName);
    sb.append(" BEFORE ");
    sb.append(op);
    sb.append(" on ");
    sb.append(tablename);
    sb.append(" FOR EACH ROW WHEN (pg_trigger_depth() < 1) EXECUTE PROCEDURE ");
    sb.append(triggerFunc);
    sb.append("();");
    stmt.addBatch(sb.toString());
    logger.info("attach trigger \n" + sb.toString());
  }
  
  protected Connection getConn()  {
    return conn;
  }

  
  protected abstract List<String> getTables();
  
  protected abstract List<String> getViews();
  
  protected void dropTables() {
    Connection conn = getConn();
    StringBuffer sb = new StringBuffer();
    sb.append("DROP TABLE IF EXISTS ");
    for (String tableName: getTables()) {
      sb.append(tableName); 
      sb.append(" ,");
    }
    sb.deleteCharAt(sb.length()-1);
    sb.append(";");
    logger.info("Deleting tables: " + sb.toString());
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sb.toString());
    } catch (SQLException e) {
      e.printStackTrace();
      logger.info("Dropping table failed");
    }
  }
  
  protected void dropViews(Statement stmt) throws SQLException {
    Connection conn = getConn();
    StringBuffer sb = new StringBuffer();
    sb.append("DROP MATERIALIZED VIEW IF EXISTS ");
    for (String viewName: getViews()) {
      sb.append(viewName); 
      sb.append(" ,");
    }
    sb.deleteCharAt(sb.length()-1);
    sb.append(";");
    logger.info("Deleting views: " + sb.toString());
    stmt.addBatch(sb.toString());
  }
  protected abstract void createViews(Statement stmt) throws SQLException;
  protected abstract void dropTriggers() throws SQLException;
  
  protected abstract void createTriggers(Statement stmt) throws SQLException;
  protected abstract void createReverseTriggers(Statement stmt) throws SQLException;
  
  protected String getTriggerName(String tablename, String triggerFunc) {
    return "SMO_" + tablename + "_" + triggerFunc;
  }
  
  protected void attachTriggers(Statement stmt, String tablename, String insertFunc, String updateFunc, String deleteFunc) 
      throws SQLException {
    attachTrigger(stmt, tablename, getTriggerName(tablename, insertFunc), insertFunc, "INSERT");
    attachTrigger(stmt, tablename, getTriggerName(tablename, deleteFunc), deleteFunc, "DELETE");
    attachTrigger(stmt, tablename, getTriggerName(tablename, updateFunc), updateFunc, "UPDATE");
  }
  
  protected void convertViews() {
    try {
      Statement stmt = conn.createStatement();
      for (String viewName : this.getViews()){
        stmt.execute("CONVERT " + viewName + ";");
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }
  
  @Override
  public void createView() {
    // Create new view based on 
    
    try {
      dropTriggers();
      conn.setAutoCommit(false);
      Statement stmt = conn.createStatement();
      dropViews(stmt);
      
      
      createTriggers(stmt);
      
      createViews(stmt);
      createReverseTriggers(stmt);
       
      stmt.executeBatch();
      conn.commit();
      conn.setAutoCommit(true);
      //convertViews();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      e.getNextException().printStackTrace();
      
      logger.warning("Creating view stage failed");
    }
  }




  @Override
  public void commitSMO() {
    // Drop tables
    
    // TODO Auto-generated method stub

  }

}
