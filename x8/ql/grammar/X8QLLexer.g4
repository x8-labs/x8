lexer grammar X8QLLexer;

options {
	caseInsensitive = true;
}

AND: 'AND';
AS: 'AS';
ASC: 'ASC';
BETWEEN: 'BETWEEN';
BY: 'BY';
COLLECTION: 'COLLECTION';
DESC: 'DESC';
END: 'END';
FALSE: 'FALSE';
FROM: 'FROM';
IN: 'IN';
INTO: 'INTO';
NOT: 'NOT';
NULL: 'NULL';
OR: 'OR';
ORDER: 'ORDER';
RANK: 'RANK';
SEARCH: 'SEARCH';
SELECT: 'SELECT';
SET: 'SET';
TRUE: 'TRUE';
WHERE: 'WHERE';

COMMA: ',';
PLUS: '+';
MINUS: '-';
STAR: '*';
DOT: '.';
QUESTION_MARK: '?';
LT: '<';
LT_EQ: '<=';
GT: '>';
GT_EQ: '>=';
EQ: '=';
NEQ: '<>' | '!=';
BRACKET_LEFT: '[';
BRACKET_RIGHT: ']';
BRACE_LEFT: '{';
BRACE_RIGHT: '}';
PAREN_LEFT: '(';
PAREN_RIGHT: ')';
COLON: ':';
SEMI_COLON: ';';
AT: '@';
SLASH: '/';

LITERAL_STRING_SINGLE
	: '\'' ( ('\'\'') | ~('\''))* '\''
	;
LITERAL_STRING_DOUBLE
	: '"' ( ('""') | ~('"'))* '"'
	;
LITERAL_INTEGER
	: [+-]? DIGIT DIGIT*
	;
LITERAL_DECIMAL
	: [+-]? DIGIT+ '.' DIGIT* ([e] [+-]? DIGIT+)?
    | [+-]? '.' DIGIT DIGIT* ([e] [+-]? DIGIT+)?
    | [+-]? DIGIT DIGIT* ([e] [+-]? DIGIT+)?
    ;
IDENTIFIER
	: [A-Z$_][A-Z0-9_-]*
	;

WS
	: WHITESPACE+ -> channel(HIDDEN)
	;
COMMENT_SINGLE_LINE
	: '--' ~[\r\n]* (('\r'? '\n') | EOF) -> channel(HIDDEN)
	;
COMMENT_MULTILINE
	: '/*' .*? '*/' -> channel(HIDDEN)
	;
UNRECOGNIZED
	: .
	;

fragment DIGIT: [0-9];
fragment LETTER: [A-Z];
fragment WHITESPACE: [ \u000B\t\r\n];
