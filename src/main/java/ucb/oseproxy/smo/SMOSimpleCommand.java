package ucb.oseproxy.smo;

import java.sql.Connection;

public abstract class SMOSimpleCommand implements SMOCommand {
  Connection conn;
  Command cmd;
  public SMOSimpleCommand() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void connect(Connection conn) {
    this.conn = conn;

  }

  @Override
  public abstract void executeSMO();

  public boolean commitSMO() {return false;}
  public boolean rollbackSMO() {return false;}
  @Override
  public boolean isReversible() {
    return true;
  } 

}
