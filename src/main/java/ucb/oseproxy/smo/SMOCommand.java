package ucb.oseproxy.smo;


public interface SMOCommand {
  
  public enum Command {
    CREATE_TABLE, DROP_TABLE, RENAME_TABLE, COPY_TABLE, MERGE_TABLE, PARTITION_TABLE,
    DECOMPOSE_TABLE,JOIN_TABLE, ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN
  }
  
  public void createView();
  
  public void commitSMO();
  
  public void rollbackSMO();
}
