package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Logger;

import ucb.oseproxy.rpc.ProxyClient;
import ucb.oseproxy.smo.SMOCommand.Command;

public class SMOCommand {
  private Command cmd;
  private List<String> args;
  private Connection conn;
  public static int MAX_ARGS = 3;
  private static final Logger logger = Logger.getLogger(ProxyClient.class.getName());

  public enum Command {
    CREATE_TABLE, DROP_TABLE, RENAME_TABLE, COPY_TABLE, MERGE_TABLE, PARTITION_TABLE,
    DECOMPOSE_TABLE,JOIN_TABLE, ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN
  }

  public SMOCommand(Connection conn, Command cmd2, List<String> options) {
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
  }
  
  private String getViewName(String tablename, int index) {
    return "OSE_VIEW" + index + "_" + tablename;
  }
  
  private boolean setupHistoryTable(Connection conn, String tablename) throws SQLException {
    // Create history table
    StringBuilder  histTable = new StringBuilder();
    histTable.append("DROP TABLE IF EXISTS ");
    histTable.append("history_" + tablename);
    histTable.append(";");
    Statement stmt = conn.createStatement();
    logger.info(histTable.toString());
    stmt.executeUpdate(histTable.toString());
    histTable = new StringBuilder();
    histTable.append("CREATE TABLE ");
    histTable.append("history_" + tablename);
    histTable.append("(op_type int ");
    ResultSet rs = stmt.executeQuery("select * from " + tablename + " where 0=1");
    ResultSetMetaData rsmd = rs.getMetaData();
    for (int i = 1; i<= rsmd.getColumnCount(); i++){
      histTable.append(",old_" + rsmd.getColumnName(i) + " " + rsmd.getColumnTypeName(i));
      histTable.append(",new_" + rsmd.getColumnName(i) + " " + rsmd.getColumnTypeName(i));
    }
    histTable.append(");");
    logger.info(histTable.toString());
    stmt.executeUpdate(histTable.toString());
    return true;
    
  }
  private String setupTriggerFunction(String tablename, String operation) throws SQLException {
    String [] ops = {"INSERT", "UPDATE", "DELETE"};
    if (!Arrays.asList(ops).contains(operation))
      return null;
    Statement stmt = conn.createStatement();

    StringBuilder triggerString  = new StringBuilder("");
    String triggerFunc = tablename + "_" + operation + "_func";
    triggerString.append("CREATE OR REPLACE FUNCTION "+ triggerFunc );
    triggerString.append(" RETURNS trigger AS $" + triggerFunc + "$");
    triggerString.append(" BEGIN ");
    ResultSet rs = stmt.executeQuery("select * from " + tablename + " where 0=1");
    ResultSetMetaData rsmd = rs.getMetaData();
    
    
    switch (operation) {
      case "INSERT":
        triggerString.append("INSERT INTO history_" + tablename + " values(");
        triggerString.append("1 ");
        for (int i = 1; i<= rsmd.getColumnCount(); i++){
          triggerString.append(", null"); // matches old_column
          triggerString.append(", NEW." + rsmd.getColumnName(i)); // matches new_column
        }
        triggerString.append(");");
        break;
      case "DELETE":
        triggerString.append("INSERT INTO history_" + tablename + " values(");
        triggerString.append("3 ");
        for (int i = 1; i<= rsmd.getColumnCount(); i++){
          triggerString.append(", OLD." + rsmd.getColumnName(i)); // matches old_column
          triggerString.append(", null"); // matches new_column
        }
        triggerString.append(");");
        break;
        
      case "UPDATE":
        triggerString.append("INSERT INTO history_" + tablename + " values(");
        triggerString.append("2 ");
        for (int i = 1; i<= rsmd.getColumnCount(); i++){
          triggerString.append(", OLD." + rsmd.getColumnName(i)); // matches old_column
          triggerString.append(", NEW." + rsmd.getColumnName(i)); // matches new_column
        }
        triggerString.append(");");
        break;
     default:
       logger.warning("Operation not supported:  " + operation);
       break;
    }
    triggerString.append(" RETURN NEW; ");
    triggerString.append("END; ");
    triggerString.append("$" + triggerFunc + "$ LANGUAGE plpgsql;");
    
    logger.info(triggerString.toString());
    stmt.executeUpdate(triggerString.toString());
    
    return triggerFunc;
  }
  
  private String getTriggerName(String tablename, String triggerFunc) {
    return "SMO_" + tablename + "_" + triggerFunc;
  }
  /* Connection in transaction mode */
  private void attachTrigger(Statement stmt, String tablename, String triggerFunc, String op) throws SQLException {
    stmt.addBatch("DROP TRIGGER IF EXISTS " + getTriggerName(tablename, triggerFunc) + " on " + tablename + ";");
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TRIGGER ");
    sb.append(getTriggerName(tablename, triggerFunc));
    sb.append(" BEFORE ");
    sb.append(op);
    sb.append(" on ");
    sb.append(tablename);
    sb.append(" FOR EACH ROW EXECUTE PROCEDURE ");
    sb.append(triggerFunc);
    sb.append("();");
    logger.info(sb.toString());
    stmt.addBatch(sb.toString());

  }
  

  public void createView() throws SQLException {
    switch (cmd) {
      case DECOMPOSE_TABLE: 
        
        // Args decompose table collista collistb collistc
        // produces views collista, collistb and collista, collistc
        if (args.size() != 4) {
          logger.warning("Number of options not valid for DECOMPOSE_TABLE");
        }
        String table = args.get(0);
        String collista = args.get(1);
        String collistb = args.get(2);
        String collistc = args.get(3);
        setupHistoryTable(conn, table);
        String insertTrigger = setupTriggerFunction(table, "INSERT");
        String updateTrigger = setupTriggerFunction(table, "UPDATE");
        String deleteTrigger = setupTriggerFunction(table, "DELETE");
        
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        attachTrigger(stmt, table, insertTrigger, "INSERT") ;
        attachTrigger(stmt, table, updateTrigger, "UPDATE");
        attachTrigger(stmt, table, deleteTrigger, "DELETE");
        
        String viewString = "CREATE MATERIALIZED VIEW %s AS select %s, %s FROM %s" ;
        String view1 = String.format(viewString, getViewName(table,1), collista, collistb , table);
        String view2 = String.format(viewString, getViewName(table,2), collista, collistc , table);
        String dropViews = "DROP MATERIALIZED VIEW IF EXISTS %s";
        String dropView1 = String.format(dropViews, getViewName(table,1));
        String dropView2 = String.format(dropViews, getViewName(table,2));
        
        stmt.addBatch(dropView1);
        stmt.addBatch(dropView2);
        stmt.addBatch(view1);
        stmt.addBatch(view2);
        stmt.executeBatch();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        logger.info("commit time" +  sdf.format(cal.getTime()));
        conn.commit();
        logger.info("after commit time" +  sdf.format(cal.getTime()));
        conn.setAutoCommit(true);
    }
    
  }
}
