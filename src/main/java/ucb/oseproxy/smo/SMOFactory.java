package ucb.oseproxy.smo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;

import java.util.List;

import ucb.oseproxy.smo.SMOCommand.Command;
import ucb.oseproxy.smo.SMOParser;
import ucb.oseproxy.smo.SMOLexer;

import org.antlr.v4.runtime.*; 
import org.antlr.v4.runtime.tree.*;



public class SMOFactory {
  
  public static SMOCommand getSMO(String cmdLine) {
    ANTLRInputStream input = new ANTLRInputStream(cmdLine);
    SMOLexer lexer = new SMOLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SMOParser parser = new SMOParser(tokens);
    ParseTree tree = parser.smo_statement_plus_semi();
    ParseTreeVisitor<SMOCommand> ptv = new SMOCommandVisitor();
    SMOCommand cmd = ptv.visit(tree);
    return cmd;
  }
  
  public static SMOCommand getSMO(Connection conn, Command cmd,  List<String> options) {
    SMOCommand smo;
    switch (cmd) {
      case DECOMPOSE_TABLE:
        smo = new SMODecompose(options);
        break;
      case MERGE_TABLE:
        smo = new SMOMerge(options);
        break;
      default:
        return null;
    }
    smo.connect(conn);
    return smo;
  }
  
  public static void main (String[] args) throws Exception {
    
    
     
  }
}
