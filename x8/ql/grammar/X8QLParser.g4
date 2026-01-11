parser grammar X8QLParser;

options {
	tokenVocab = X8QLLexer;
}

// Allow selected keywords to be used where identifiers are expected (fields, params, etc.)
// Excludes boolean/null and operator-like keywords (AND, OR, NOT, BETWEEN, IN) to avoid ambiguity.
identifier
	: IDENTIFIER
	| SELECT | FROM | INTO | COLLECTION | ORDER | BY | WHERE | SEARCH | SET | ASC | DESC | RANK
	;

parse_statement
	: statement EOF
	;

parse_search
	: expression EOF
	;

parse_where
	: expression EOF
	;

parse_select
	: select EOF
	;

parse_collection
	: collection EOF
	;

parse_order_by
	: order_by EOF
	;

parse_rank_by
	: expression EOF
	;

parse_update
	: update EOF
	;

statement
	: op=identifier (clause)*       									# statement_single
	| op=identifier statement SEMI_COLON (statement SEMI_COLON)* END	# statement_multi
	;

clause
	: select_clause	
	| collection_clause
	| set_clause
	| search_clause
	| where_clause
	| order_by_clause
	| rank_by_clause
	| generic_clause
	;

generic_clause
	: name=identifier operand
	;

select_clause
	: SELECT select
	;

select
	: STAR 								# select_all
	| select_term (COMMA select_term)* 	# select_terms
	| parameter							# select_parameter
	;

select_term
	: field (AS field)?
	;

collection_clause
	: (COLLECTION | FROM | INTO) collection
	;

collection
	: identifier (DOT identifier)*		# collection_identifier
	| parameter							# collection_parameter
	;

search_clause
	: SEARCH expression
	;

where_clause
	: WHERE expression
	;

expression
	: operand																		# expression_operand
	| PAREN_LEFT expression PAREN_RIGHT												# expression_paranthesis
	| lhs=operand op=(EQ | NEQ | GT | GT_EQ | LT | LT_EQ) rhs=operand				# expression_comparison
	| lhs=operand BETWEEN low=operand AND high=operand								# expression_comparison_between
	| lhs=operand (not_in=NOT)? IN 
		PAREN_LEFT operand (COMMA operand)* PAREN_RIGHT								# expression_comparison_in
	| NOT expression																# expression_not
	| lhs=expression AND rhs=expression												# expression_and
	| lhs=expression OR rhs=expression												# expression_or
	;

operand
	: value				# operand_value
	| field				# operand_field
	| parameter			# operand_parameter
	| ref				# operand_ref
	| function			# operand_function
	;

order_by_clause
	: ORDER BY order_by
	;

order_by
	: order_by_term (COMMA order_by_term)*		# order_by_terms
	| parameter									# order_by_parameter
	;

order_by_term
	: field direction=(ASC | DESC)?
	;

rank_by_clause
	: RANK BY expression
	;

set_clause
	: SET update
	;

update
	: update_operation (COMMA update_operation)*		# update_operations
	| parameter											# update_parameter
	;

update_operation
	: field EQ function
	;

function
	: (namespace=identifier DOT)? name=identifier function_args
	;

function_args
	: PAREN_LEFT PAREN_RIGHT									# function_no_args
	| PAREN_LEFT operand (COMMA operand)* PAREN_RIGHT 			# function_with_args
	| PAREN_LEFT named_arg (COMMA named_arg)* PAREN_RIGHT		# function_with_named_args
	;

named_arg
	: name=identifier EQ operand
	;

ref
	: BRACE_LEFT BRACE_LEFT path=ref_path BRACE_RIGHT BRACE_RIGHT
	;


ref_path
	: (identifier COLON SLASH SLASH)? identifier DOT field
	;


parameter
	: AT name=identifier
	;

field
	: field_primitive (field_path)*
	;

field_path
	: BRACKET_LEFT value BRACKET_RIGHT
	| BRACKET_LEFT MINUS BRACKET_RIGHT
	| DOT field_primitive
	;


field_primitive
	: identifier
	;

value
	: NULL						# value_null
	| TRUE 						# value_true
	| FALSE 					# value_false
	| literal_string 			# value_string
	| LITERAL_INTEGER			# value_integer
	| LITERAL_DECIMAL  			# value_decimal
	| json						# value_json
	| array						# value_array
	;

literal_string
	: LITERAL_STRING_SINGLE
	| LITERAL_STRING_DOUBLE
	;

array
	: BRACKET_LEFT BRACKET_RIGHT							# array_empty
	| BRACKET_LEFT value (COMMA value)* BRACKET_RIGHT		# array_items
	;

/*
 * JSON grammar
 */

json
	: json_value
	;

json_obj
	: BRACE_LEFT json_pair (COMMA json_pair)* BRACE_RIGHT
	| BRACE_LEFT BRACE_RIGHT
	;

json_pair
	: json_string COLON json_value
	;

json_arr
	: BRACKET_LEFT json_value (COMMA json_value)* BRACKET_RIGHT
	| BRACKET_LEFT BRACKET_RIGHT
	;

json_value
	: json_string
	| json_number
	| json_obj
	| json_arr
	| TRUE
	| FALSE
	| NULL
	;

json_string
	: LITERAL_STRING_DOUBLE
	;

json_number
	: LITERAL_INTEGER
	| LITERAL_DECIMAL
	;