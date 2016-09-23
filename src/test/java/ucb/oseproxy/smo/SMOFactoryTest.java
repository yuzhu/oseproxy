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
    
    String testMergeTable = "MERGE TABLE cur_text, old_text  INTO text;";
    smo = SMOFactory.getSMO(testMergeTable);
    assertTrue(smo instanceof SMOMerge);
    assertTrue(((SMOMerge)smo).getTables().get(0).equals("cur_text"));
    assertTrue(((SMOMerge)smo).getTables().get(1).equals("old_text"));
    assertTrue(((SMOMerge)smo).getViews().get(0).equals("text"));
    
    
    //assertTrue(false);
    
    
    //fail("Not yet implemented");
  }

}
