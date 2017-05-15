grammar SMO;
prog: smo_statement_plus_semi* EOF ;

swallow_to_semi
    : ~( ';' )+
    ;

smo_statement_plus_semi : smo_statement ';' ;
smo_statement
  :DROP_TABLE ID        # droptable
  |CREATE_TABLE ID (bracketlist)?  # createtable
  |RENAME_TABLE ID INTO ID    # renametable
  |COPY_TABLE ID INTO ID    # copytable
  |MERGE_TABLE ID COMMA ID INTO ID   # mergetable
  |PARTITION_TABLE ID INTO ID COMMA ID WHERE swallow_to_semi  # partitiontable
  |DECOMPOSE_TABLE ID INTO bracketlist COMMA ID bracketlist COMMA ID bracketlist # decomposetable
  |JOIN_TABLE ID COMMA ID INTO ID WHERE swallow_to_semi   #jointable
  |ADD_COLUMN ID (AS expr)? INTO ID  #addcolumn
  |DROP_COLUMN ID FROM ID # dropcolumn
  |RENAME_COLUMN ID IN ID TO ID #renamecolumn
  |COPY_COLUMN ID FROM ID INTO ID (WHERE swallow_to_semi)? #copycolumn
  |NOP #noop
  ;

columnlist: (ID) (COMMA ID)*;
bracketlist:'(' columnlist ')';
paramlist:'()' | bracketlist;
expr : function | STRING_LITERAL | NULL;
function: ID paramlist;

STRING_LITERAL: '"' (~('"' | '\r' | '\n') | '"' '"' | NEWLINE)*  '"';

DROP_TABLE: 'DROP TABLE';
CREATE_TABLE: 'CREATE TABLE';
RENAME_TABLE: 'RENAME TABLE';
COPY_TABLE: 'COPY TABLE';
MERGE_TABLE: 'MERGE TABLE';
PARTITION_TABLE: 'PARTITION TABLE';
DECOMPOSE_TABLE: 'DECOMPOSE TABLE';
JOIN_TABLE: 'JOIN TABLE';
ADD_COLUMN: 'ADD COLUMN';
DROP_COLUMN: 'DROP COLUMN';
RENAME_COLUMN: 'RENAME COLUMN';
COPY_COLUMN: 'COPY COLUMN';
NOP: 'NOP';


AS: 'AS';
INTO: 'INTO';
NULL: 'null';
FROM: 'FROM';
IN: 'IN';
TO: 'TO';
COMMA: ',';
WHERE: 'WHERE';

ID
    : (SIMPLE_LETTER) (SIMPLE_LETTER | '$' | '_' | '#' | ('0'..'9'))*;

SIMPLE_LETTER
    : 'a'..'z'
    | 'A'..'Z'
    ;
LINE_COMMENT
    :   '//' ~[\r\n]* -> skip
    ;
SPACES
    : [ \t\n\r]+ -> skip
    ;
ANYCHAR : (~[\r\n]);
NEWLINE : [\r\n]+ ;


