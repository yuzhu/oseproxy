package ucb.oseproxy.smo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SMODropTable extends SMOAbstractCommand {
  
  public SMODropTable() {
    // TODO Auto-generated constructor stub
  }

  public SMODropTable(String tablename) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rollbackSMO() {
    // TODO Auto-generated method stub

  }

  @Override
  protected List<String> getTables() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected List<String> getViews() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void createViews(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void dropTriggers() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void createTriggers(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void createReverseTriggers(Statement stmt) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commitSMO() {
    // TODO Auto-generated method stub

  }

}
