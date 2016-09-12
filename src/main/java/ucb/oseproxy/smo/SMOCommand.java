package ucb.oseproxy.smo;

import java.sql.Connection;
import java.util.List;

public interface SMOCommand {
  
  public enum Command {
    CREATE_TABLE, DROP_TABLE, RENAME_TABLE, COPY_TABLE, MERGE_TABLE, PARTITION_TABLE,
    DECOMPOSE_TABLE,JOIN_TABLE, ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN
  }

  public void connect(Connection conn);
  
  public void executeSMO();
  
  public boolean commitSMO();
  
  public boolean rollbackSMO();
}
