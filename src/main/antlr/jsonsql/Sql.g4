grammar Sql;

@header {
 package jsonsql;
}

stmt
  : select_stmt ';'
  | describe_stmt ';'
  | EXPLAIN select_stmt ';'
  ;

select_stmt
  : SELECT named_expr ( ',' named_expr )* FROM table_or_subquery predicate? group_by? order_by? (LIMIT NUMERIC_LITERAL)?
  ;

named_expr
  : expr
  | expr AS IDENTIFIER
  ;

group_by
  : GROUP BY expr ( ',' expr )*
  ;

order_by_expr
  : expr
  | expr DESC
  | expr ASC
  ;

order_by
  : ORDER BY order_by_expr ( ',' order_by_expr )*
  ;

predicate
  : WHERE expr
  ;

describe_stmt
  : DESCRIBE table
  ;

expr
  : '(' expr ')'
  | expr OP_IDX expr ']'
  | expr OP_DOT IDENTIFIER
  | STRING_LITERAL
  | NUMERIC_LITERAL
  | function_call
  | IDENTIFIER
  | expr ( OP_MULT | OP_DIV ) expr
  | expr ( OP_PLUS | OP_MINUS ) expr
  | expr ( OP_GT | OP_GTE | OP_LT | OP_LTE ) expr
  | expr ( OP_EQ | OP_NEQ ) expr
  | expr IS NULL
  | expr IS NOT NULL
  | expr ( OP_AND | OP_OR ) expr
  ;

table
  : JSON STRING_LITERAL
  ;

subquery
  : '(' select_stmt ')'
  ;

lateral_view
  : LATERAL VIEW named_expr
  ;

table_or_subquery
  : table
  | subquery
  | table_or_subquery lateral_view
  ;

function_call
  : IDENTIFIER '(' (expr ( ',' expr )*)? ')'
  ;

// Key words
DESCRIBE: D E S C R I B E;
SELECT: S E L E C T;
JSON: J S O N;
FROM: F R O M;
LIMIT: L I M I T;
WHERE: W H E R E;
EXPLAIN: E X P L A I N;
GROUP: G R O U P;
ORDER: O R D E R;
BY: B Y;
AS: A S;
ASC: A S C;
DESC: D E S C;
IS: I S;
NOT: N O T;
NULL: N U L L;
LATERAL: L A T E R A L;
VIEW: V I E W;


OP_PLUS: '+';
OP_MINUS: '-';
OP_MULT: '*';
OP_DIV: '/';
OP_GT: '>';
OP_GTE: '>=';
OP_LT: '<';
OP_LTE: '<=';
OP_EQ: '=' | '==';
OP_NEQ: '!=' | '<>';
OP_DOT: '.';
OP_IDX: '[';
OP_AND: A N D;
OP_OR: O R;


IDENTIFIER
 : [a-zA-Z_] [a-zA-Z_0-9]*
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

// Literals
NUMERIC_LITERAL
 : '-'? DIGIT+ ( '.' DIGIT* )?
 ;

STRING_LITERAL
 : '\'' ( ~'\'' )* '\''
 | '"' ( ~'"' )* '"'
 ;

// Whitespace
SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

// Fragments
fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
