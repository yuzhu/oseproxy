package ucb.oseproxy.smo;

import java.util.List;

import junit.framework.TestCase;

public class SMOFactoryTest extends TestCase {

  public SMOFactoryTest(String name) {
    super(name);
  }

  public void testGetSMOString() {
    String testDropTable = "DROP TABLE user_newtalk;";
    SMOCommand smo = SMOFactory.getSMO(testDropTable);
    assertFalse (smo == null);
    assertTrue(smo instanceof SMODropTable);
    SMODropTable smodt = (SMODropTable) smo;
    assertTrue(smodt.getTableName().equals("user_newtalk"));
    
    String testCreateTable = "CREATE TABLE user_newtalk(user_id, user_ip);";
    smo = SMOFactory.getSMO(testCreateTable);
    assertFalse (smo == null);
    assertTrue(smo instanceof SMOCreateTable);
    SMOCreateTable smoct = (SMOCreateTable) smo;
    assertTrue(smoct.getTableName().equals("user_newtalk"));
    List<String> collist = smoct.getColumnList();
    assertTrue(collist.get(0).equals("user_id"));
    assertTrue(collist.get(1).equals("user_ip"));
    
    String testRenameTable = "RENAME TABLE cur INTO cur_text;";
    smo = SMOFactory.getSMO(testRenameTable);
    assertFalse (smo == null);
    assertTrue(smo instanceof SMORenameTable);
    SMORenameTable smort = (SMORenameTable) smo;
    assertTrue(smort.getFromName().equals("cur"));
    assertTrue(smort.getToName().equals("cur_text"));
    
    
    String testPartitionTable = "PARTITION TABLE table INTO table1, table2 WHERE table.col1 = 'OLD';";
    smo = SMOFactory.getSMO(testPartitionTable);
    assertTrue(smo instanceof SMOPartitionTable);
    assertTrue(((SMOPartitionTable)smo).getTables().get(0).equals("table"));
    assertTrue(((SMOPartitionTable)smo).getViews().get(0).equals("table1"));
    assertTrue(((SMOPartitionTable)smo).getViews().get(1).equals("table2"));
   
    assertTrue(((SMOPartitionTable)smo).getCond().equals("table.col1='OLD'"));
    
    String testJoinTable = "JOIN TABLE table1, table2 INTO table WHERE table1.col1 = table2.col2;";
    smo = SMOFactory.getSMO(testJoinTable);
    assertTrue(smo instanceof SMOJoinTable);
    assertTrue(((SMOJoinTable)smo).getTables().get(0).equals("table1"));
    assertTrue(((SMOJoinTable)smo).getTables().get(1).equals("table2"));
    assertTrue(((SMOJoinTable)smo).getViews().get(0).equals("table"));
    
    assertTrue(((SMOJoinTable)smo).getCond().equals("table1.col1=table2.col2"));
    //assertTrue(false);
    
    
    //fail("Not yet implemented");
  }

}
