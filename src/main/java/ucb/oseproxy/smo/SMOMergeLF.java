package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;


// MERGE TABLE (R,S) INTO T, assuming R, S has the same structure
/**
 * @author yuzhu
 *
 */
public class SMOMergeLF extends SMOAbstractCommand {

  private static Logger logger = Logger.getLogger(SMOAbstractCommand.class.getName());
  private String r,s,t;
  private List<String> tables, views;
  private List<String> fields = null;
  
  
  public void init(String r, String s, String t) {
    this.r=r;
    this.s=s;
    this.t=t;
    tables = new ArrayList<String>();
    views = new ArrayList<String>();
    tables.add(r);
    tables.add(s);
    views.add(t);
    
  }
  
  public void connect(Connection conn){
    super.connect(conn);
    // populate fields list
    if (fields == null){
      fields = Utils.getColumns(conn, r);
    }
  }

  public SMOMergeLF(String r, String s, String t) {
    this.cmd = Command.MERGE_TABLE;
    init(r,s,t);
  }
  
  public SMOMergeLF(List<String> options) {
    this.cmd = Command.MERGE_TABLE;
    if (options.size() != 3) {
      logger.warning("Number of options not valid for MERGE_TABLE");
    }
    init(options.get(0), options.get(1),options.get(2));
  }

  @Override
  protected List<String> getTables() {
    return tables;
  }

  @Override
  protected List<String> getViews() {
    return views;
  }

  @Override
  protected void createViews(Statement stmt) throws SQLException {
    String viewString = "CREATE MATERIALIZED VIEW %s AS select * FROM %s UNION ALL select * FROM %s;";
    String view1 = String.format(viewString, t, r, s);
    logger.info(view1);
    
    stmt.execute(view1);  
    logger.info("view created");
    
  }

  private void createTrigger(Statement stmt, String triggerName, String tableName, int iteration) throws SQLException{
    String triggerTemplate = "CREATE TRIGGER %s"
        + " AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE trigger_async_log(%d)";
    String triggerString = String.format(triggerTemplate, triggerName, tableName, iteration);
    stmt.addBatch(triggerString);
  }
  
  protected void createTriggers(Connection conn, int iteration) throws SQLException {
    

    Statement stmt = conn.createStatement();
    
    for (String table : this.getTables()){
      createTrigger(stmt, "trigger_async", table, iteration);
    }
    stmt.executeBatch();
    
  }
  
  private int propHistory(Connection conn, int iteration) throws SQLException {
    int count = 0;
    for (String table : this.getTables()){
      for (String view: this.getViews()) {
        count += propHistory(conn, table, view, iteration);
      }
    }
    return count;
  }
  
  private int propHistory(Connection conn, String tableName, String viewName, int iteration) throws SQLException {
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    String checkNumber = "select count(*) from migration.hist_log where schema_name = 'public' and table_name ='"
                + tableName +"' and iteration = " + iteration +" ;";
    ResultSet rs = stmt.executeQuery(checkNumber);
    if (!rs.next()) {
      logger.warning("count measure returned null");
    }
    int count = rs.getInt(1);
    
    if (count > 0) {
      String prop = "select play_log(schema_name, table_name, action, old_data, new_data, 'public', '"+ viewName + "') "
          + "from migration.hist_log where schema_name = 'public' and table_name ='"
                + tableName +"' and iteration = " + iteration +" ;";
      stmt.executeQuery(prop);
    }
    conn.commit();
    conn.setAutoCommit(true);
    return count;
    
  }

  @Override
  protected void createReverseTriggers(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub
    
  }
  
  
  protected void dropTriggers() throws SQLException {
    // String[] ops = {"INSERT", "UPDATE", "DELETE"};
    Statement stmt = conn.createStatement();
    for (String tablename : this.getTables()) {
        String query = "DROP TRIGGER IF EXISTS " + "trigger_async" + " on " + tablename + ";";
        logger.info(query);
        stmt.executeUpdate(query);
      }
    }
  public void executeSMO() {
    // Create new view based on 
    
    try {
      dropTriggers();
      //conn.setAutoCommit(false);
      Statement stmt = conn.createStatement();
      dropViews(stmt);
      
      int iteration = 1;
      createTriggers(conn, iteration);
      
      createViews(stmt);
     
      while (propHistory (conn, iteration) > 0 ){
        conn.setAutoCommit(false);
        dropTriggers(); // needs to change
        iteration ++;
        createTriggers(conn,  iteration);
        conn.commit();
        conn.setAutoCommit(true);
      }
      //createReverseTriggers(stmt);
       
      // stmt.executeBatch();
      // conn.commit();
      conn.setAutoCommit(true);
      convertViews();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      e.getNextException().printStackTrace();
      
      logger.warning("Creating view stage failed");
    }
  }

  @Override
  protected void createTriggers(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub
    
  }
  
  
}

