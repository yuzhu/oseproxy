package ucb.oseproxy.smo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SMOCommandVisitor extends SMOBaseVisitor<SMOCommand> {
  
  public SMOCommand visitDroptable(SMOParser.DroptableContext ctx) {
    SMOCommand cmd = new SMODropTable(ctx.ID().getText());
    return cmd;
  }
  
  public SMOCommand visitMergetable(SMOParser.MergetableContext ctx) {
    List<String> strings = ctx.ID().stream()
        .map(object -> object.getText())
        .collect(Collectors.toList());
    
    return new SMOMerge(strings);
  }
  
  public SMOCommand visitDecomposetable(SMOParser.DecomposetableContext ctx){
    List<String> options = new ArrayList<String>(6);
    options.add(ctx.ID(0).getText());
    return new SMODecompose(ctx.ID(0).getText(), ctx.ID(1).getText(), ctx.ID(2).getText(),
        ctx.bracketlist(0).columnlist().getText(), ctx.bracketlist(1).columnlist().getText()
        );
  }
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
