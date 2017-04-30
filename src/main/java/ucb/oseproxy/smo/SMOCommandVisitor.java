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
    if (ctx.bracketlist() == null || ctx.bracketlist().columnlist() == null)
      return new SMOCreateTable(ctx.ID().getText(), "");
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
  
  public SMOCommand visitAddcolumn(SMOParser.AddcolumnContext ctx) {
    return new SMOAddColumn(ctx.ID(0).getText(), ctx.expr().getText(), ctx.ID(1).getText());
  }
  
  public SMOCommand visitDropcolumn(SMOParser.DropcolumnContext ctx) {
    return new SMODropColumn(ctx.ID(0).getText(), ctx.ID(1).getText());
  }
  
  
  
  public SMOCommand visitRenamecolumn(SMOParser.RenamecolumnContext ctx) {
    return new SMORenameColumn(ctx.ID(0).getText(), ctx.ID(2).getText(), ctx.ID(1).getText());
  }
  
  public SMOCommand visitCopycolumn(SMOParser.CopycolumnContext ctx) {
    if (ctx.swallow_to_semi()!= null)
      return new SMOCopyColumn(ctx.ID(0).getText(), ctx.ID(1).getText(), ctx.ID(2).getText(),
        ctx.swallow_to_semi().getText() );
    else 
      return new SMOCopyColumn(ctx.ID(0).getText(), ctx.ID(1).getText(), ctx.ID(2).getText(),
          "");
    
  }
  
  public SMOCommand visitNop(SMOParser.NoopContext ctx) {
    return new SMONop();
  }
 
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
