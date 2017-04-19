package ucb.oseproxy.smo;

import java.util.ArrayList;
import java.util.List;

public class SMOCommandVisitor extends SMOBaseVisitor<SMOCommand> {
  
  
  public SMOCommand visitSmo_statement_plus_semi(SMOParser.Smo_statement_plus_semiContext ctx){
    return visit(ctx.smo_statement());
  }
  
  public SMOCommand visitDroptable(SMOParser.DroptableContext ctx) {
    SMOCommand cmd = new SMODropTable(ctx.ID().getText());
    return cmd;
  }
  
  public SMOCommand visitCreatetable(SMOParser.CreatetableContext ctx){
    SMOCommand cmd = new SMOCreateTable(ctx.ID().getText(), ctx.bracketlist().columnlist().getText());
    return cmd;
  }
  
  public SMOCommand visitRenametable(SMOParser.RenametableContext ctx){
    SMOCommand cmd = new SMORenameTable(ctx.ID(0).getText(), ctx.ID(1).getText());
    return cmd;
  }
  
  public SMOCommand visitCopytable(SMOParser.CopytableContext ctx) {
    SMOCommand cmd = new SMOCopyTable(ctx.ID(0).getText(), ctx.ID(1).getText());
    return cmd;
  }
  
  public SMOCommand visitPartitiontable(SMOParser.PartitiontableContext ctx){
    SMOCommand cmd = new SMOPartitionTable(ctx.ID(0).getText(), ctx.ID(1).getText(), 
        ctx.ID(2).getText(), ctx.swallow_to_semi().getText());
    return cmd;
  }
  
  public SMOCommand visitJointable(SMOParser.JointableContext ctx){
    SMOCommand cmd = new SMOJoinTable(ctx.ID(0).getText(),ctx.ID(1).getText(), 
        ctx.ID(2).getText(),ctx.swallow_to_semi().getText());
    return cmd;
  }
  
  public SMOCommand visitMergetable(SMOParser.MergetableContext ctx) {
    
    return new SMOMerge(ctx.ID(0).getText(), ctx.ID(1).getText(), ctx.ID(2).getText());
  }
  
  public SMOCommand visitDecomposetable(SMOParser.DecomposetableContext ctx){
    List<String> options = new ArrayList<String>(6);
    options.add(ctx.ID(0).getText());
    return new SMODecompose(ctx.ID(0).getText(), ctx.ID(1).getText(), ctx.ID(2).getText(),
        ctx.bracketlist(0).columnlist().getText(), ctx.bracketlist(1).columnlist().getText()
        );
  }
  
  public SMOCommand visitAddColumn(SMOParser.AddcolumnContext ctx) {
   
    return new SMOAddColumn(ctx.ID(0).getText(), ctx.expr().getText(), ctx.ID(1).getText());
  }
  
  public SMOCommand visitDropColumn(SMOParser.DropcolumnContext ctx) {
    return null;
  }
  
  public SMOCommand visitRenameColumn(SMOParser.RenamecolumnContext ctx) {
    return null;
  }
  
 
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
