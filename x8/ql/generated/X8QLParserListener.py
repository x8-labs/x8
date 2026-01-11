# flake8: noqa
# type: ignore
# Generated from X8QLParser.g4 by ANTLR 4.13.2
from antlr4 import *

if "." in __name__:
    from .X8QLParser import X8QLParser
else:
    from x8.ql.generated.X8QLParser import X8QLParser


# This class defines a complete listener for a parse tree produced by X8QLParser.
class X8QLParserListener(ParseTreeListener):

    # Enter a parse tree produced by X8QLParser#identifier.
    def enterIdentifier(self, ctx: X8QLParser.IdentifierContext):
        pass

    # Exit a parse tree produced by X8QLParser#identifier.
    def exitIdentifier(self, ctx: X8QLParser.IdentifierContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_statement.
    def enterParse_statement(self, ctx: X8QLParser.Parse_statementContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_statement.
    def exitParse_statement(self, ctx: X8QLParser.Parse_statementContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_search.
    def enterParse_search(self, ctx: X8QLParser.Parse_searchContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_search.
    def exitParse_search(self, ctx: X8QLParser.Parse_searchContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_where.
    def enterParse_where(self, ctx: X8QLParser.Parse_whereContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_where.
    def exitParse_where(self, ctx: X8QLParser.Parse_whereContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_select.
    def enterParse_select(self, ctx: X8QLParser.Parse_selectContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_select.
    def exitParse_select(self, ctx: X8QLParser.Parse_selectContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_collection.
    def enterParse_collection(self, ctx: X8QLParser.Parse_collectionContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_collection.
    def exitParse_collection(self, ctx: X8QLParser.Parse_collectionContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_order_by.
    def enterParse_order_by(self, ctx: X8QLParser.Parse_order_byContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_order_by.
    def exitParse_order_by(self, ctx: X8QLParser.Parse_order_byContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_rank_by.
    def enterParse_rank_by(self, ctx: X8QLParser.Parse_rank_byContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_rank_by.
    def exitParse_rank_by(self, ctx: X8QLParser.Parse_rank_byContext):
        pass

    # Enter a parse tree produced by X8QLParser#parse_update.
    def enterParse_update(self, ctx: X8QLParser.Parse_updateContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_update.
    def exitParse_update(self, ctx: X8QLParser.Parse_updateContext):
        pass

    # Enter a parse tree produced by X8QLParser#statement_single.
    def enterStatement_single(self, ctx: X8QLParser.Statement_singleContext):
        pass

    # Exit a parse tree produced by X8QLParser#statement_single.
    def exitStatement_single(self, ctx: X8QLParser.Statement_singleContext):
        pass

    # Enter a parse tree produced by X8QLParser#statement_multi.
    def enterStatement_multi(self, ctx: X8QLParser.Statement_multiContext):
        pass

    # Exit a parse tree produced by X8QLParser#statement_multi.
    def exitStatement_multi(self, ctx: X8QLParser.Statement_multiContext):
        pass

    # Enter a parse tree produced by X8QLParser#clause.
    def enterClause(self, ctx: X8QLParser.ClauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#clause.
    def exitClause(self, ctx: X8QLParser.ClauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#generic_clause.
    def enterGeneric_clause(self, ctx: X8QLParser.Generic_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#generic_clause.
    def exitGeneric_clause(self, ctx: X8QLParser.Generic_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#select_clause.
    def enterSelect_clause(self, ctx: X8QLParser.Select_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_clause.
    def exitSelect_clause(self, ctx: X8QLParser.Select_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#select_all.
    def enterSelect_all(self, ctx: X8QLParser.Select_allContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_all.
    def exitSelect_all(self, ctx: X8QLParser.Select_allContext):
        pass

    # Enter a parse tree produced by X8QLParser#select_terms.
    def enterSelect_terms(self, ctx: X8QLParser.Select_termsContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_terms.
    def exitSelect_terms(self, ctx: X8QLParser.Select_termsContext):
        pass

    # Enter a parse tree produced by X8QLParser#select_parameter.
    def enterSelect_parameter(self, ctx: X8QLParser.Select_parameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_parameter.
    def exitSelect_parameter(self, ctx: X8QLParser.Select_parameterContext):
        pass

    # Enter a parse tree produced by X8QLParser#select_term.
    def enterSelect_term(self, ctx: X8QLParser.Select_termContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_term.
    def exitSelect_term(self, ctx: X8QLParser.Select_termContext):
        pass

    # Enter a parse tree produced by X8QLParser#collection_clause.
    def enterCollection_clause(self, ctx: X8QLParser.Collection_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#collection_clause.
    def exitCollection_clause(self, ctx: X8QLParser.Collection_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#collection_identifier.
    def enterCollection_identifier(
        self, ctx: X8QLParser.Collection_identifierContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#collection_identifier.
    def exitCollection_identifier(
        self, ctx: X8QLParser.Collection_identifierContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#collection_parameter.
    def enterCollection_parameter(
        self, ctx: X8QLParser.Collection_parameterContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#collection_parameter.
    def exitCollection_parameter(
        self, ctx: X8QLParser.Collection_parameterContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#search_clause.
    def enterSearch_clause(self, ctx: X8QLParser.Search_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#search_clause.
    def exitSearch_clause(self, ctx: X8QLParser.Search_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#where_clause.
    def enterWhere_clause(self, ctx: X8QLParser.Where_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#where_clause.
    def exitWhere_clause(self, ctx: X8QLParser.Where_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#expression_operand.
    def enterExpression_operand(
        self, ctx: X8QLParser.Expression_operandContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_operand.
    def exitExpression_operand(
        self, ctx: X8QLParser.Expression_operandContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#expression_not.
    def enterExpression_not(self, ctx: X8QLParser.Expression_notContext):
        pass

    # Exit a parse tree produced by X8QLParser#expression_not.
    def exitExpression_not(self, ctx: X8QLParser.Expression_notContext):
        pass

    # Enter a parse tree produced by X8QLParser#expression_or.
    def enterExpression_or(self, ctx: X8QLParser.Expression_orContext):
        pass

    # Exit a parse tree produced by X8QLParser#expression_or.
    def exitExpression_or(self, ctx: X8QLParser.Expression_orContext):
        pass

    # Enter a parse tree produced by X8QLParser#expression_comparison.
    def enterExpression_comparison(
        self, ctx: X8QLParser.Expression_comparisonContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_comparison.
    def exitExpression_comparison(
        self, ctx: X8QLParser.Expression_comparisonContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#expression_comparison_in.
    def enterExpression_comparison_in(
        self, ctx: X8QLParser.Expression_comparison_inContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_comparison_in.
    def exitExpression_comparison_in(
        self, ctx: X8QLParser.Expression_comparison_inContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#expression_paranthesis.
    def enterExpression_paranthesis(
        self, ctx: X8QLParser.Expression_paranthesisContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_paranthesis.
    def exitExpression_paranthesis(
        self, ctx: X8QLParser.Expression_paranthesisContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#expression_comparison_between.
    def enterExpression_comparison_between(
        self, ctx: X8QLParser.Expression_comparison_betweenContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_comparison_between.
    def exitExpression_comparison_between(
        self, ctx: X8QLParser.Expression_comparison_betweenContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#expression_and.
    def enterExpression_and(self, ctx: X8QLParser.Expression_andContext):
        pass

    # Exit a parse tree produced by X8QLParser#expression_and.
    def exitExpression_and(self, ctx: X8QLParser.Expression_andContext):
        pass

    # Enter a parse tree produced by X8QLParser#operand_value.
    def enterOperand_value(self, ctx: X8QLParser.Operand_valueContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_value.
    def exitOperand_value(self, ctx: X8QLParser.Operand_valueContext):
        pass

    # Enter a parse tree produced by X8QLParser#operand_field.
    def enterOperand_field(self, ctx: X8QLParser.Operand_fieldContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_field.
    def exitOperand_field(self, ctx: X8QLParser.Operand_fieldContext):
        pass

    # Enter a parse tree produced by X8QLParser#operand_parameter.
    def enterOperand_parameter(self, ctx: X8QLParser.Operand_parameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_parameter.
    def exitOperand_parameter(self, ctx: X8QLParser.Operand_parameterContext):
        pass

    # Enter a parse tree produced by X8QLParser#operand_ref.
    def enterOperand_ref(self, ctx: X8QLParser.Operand_refContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_ref.
    def exitOperand_ref(self, ctx: X8QLParser.Operand_refContext):
        pass

    # Enter a parse tree produced by X8QLParser#operand_function.
    def enterOperand_function(self, ctx: X8QLParser.Operand_functionContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_function.
    def exitOperand_function(self, ctx: X8QLParser.Operand_functionContext):
        pass

    # Enter a parse tree produced by X8QLParser#order_by_clause.
    def enterOrder_by_clause(self, ctx: X8QLParser.Order_by_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_clause.
    def exitOrder_by_clause(self, ctx: X8QLParser.Order_by_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#order_by_terms.
    def enterOrder_by_terms(self, ctx: X8QLParser.Order_by_termsContext):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_terms.
    def exitOrder_by_terms(self, ctx: X8QLParser.Order_by_termsContext):
        pass

    # Enter a parse tree produced by X8QLParser#order_by_parameter.
    def enterOrder_by_parameter(
        self, ctx: X8QLParser.Order_by_parameterContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_parameter.
    def exitOrder_by_parameter(
        self, ctx: X8QLParser.Order_by_parameterContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#order_by_term.
    def enterOrder_by_term(self, ctx: X8QLParser.Order_by_termContext):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_term.
    def exitOrder_by_term(self, ctx: X8QLParser.Order_by_termContext):
        pass

    # Enter a parse tree produced by X8QLParser#rank_by_clause.
    def enterRank_by_clause(self, ctx: X8QLParser.Rank_by_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#rank_by_clause.
    def exitRank_by_clause(self, ctx: X8QLParser.Rank_by_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#set_clause.
    def enterSet_clause(self, ctx: X8QLParser.Set_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#set_clause.
    def exitSet_clause(self, ctx: X8QLParser.Set_clauseContext):
        pass

    # Enter a parse tree produced by X8QLParser#update_operations.
    def enterUpdate_operations(self, ctx: X8QLParser.Update_operationsContext):
        pass

    # Exit a parse tree produced by X8QLParser#update_operations.
    def exitUpdate_operations(self, ctx: X8QLParser.Update_operationsContext):
        pass

    # Enter a parse tree produced by X8QLParser#update_parameter.
    def enterUpdate_parameter(self, ctx: X8QLParser.Update_parameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#update_parameter.
    def exitUpdate_parameter(self, ctx: X8QLParser.Update_parameterContext):
        pass

    # Enter a parse tree produced by X8QLParser#update_operation.
    def enterUpdate_operation(self, ctx: X8QLParser.Update_operationContext):
        pass

    # Exit a parse tree produced by X8QLParser#update_operation.
    def exitUpdate_operation(self, ctx: X8QLParser.Update_operationContext):
        pass

    # Enter a parse tree produced by X8QLParser#function.
    def enterFunction(self, ctx: X8QLParser.FunctionContext):
        pass

    # Exit a parse tree produced by X8QLParser#function.
    def exitFunction(self, ctx: X8QLParser.FunctionContext):
        pass

    # Enter a parse tree produced by X8QLParser#function_no_args.
    def enterFunction_no_args(self, ctx: X8QLParser.Function_no_argsContext):
        pass

    # Exit a parse tree produced by X8QLParser#function_no_args.
    def exitFunction_no_args(self, ctx: X8QLParser.Function_no_argsContext):
        pass

    # Enter a parse tree produced by X8QLParser#function_with_args.
    def enterFunction_with_args(
        self, ctx: X8QLParser.Function_with_argsContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#function_with_args.
    def exitFunction_with_args(
        self, ctx: X8QLParser.Function_with_argsContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#function_with_named_args.
    def enterFunction_with_named_args(
        self, ctx: X8QLParser.Function_with_named_argsContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#function_with_named_args.
    def exitFunction_with_named_args(
        self, ctx: X8QLParser.Function_with_named_argsContext
    ):
        pass

    # Enter a parse tree produced by X8QLParser#named_arg.
    def enterNamed_arg(self, ctx: X8QLParser.Named_argContext):
        pass

    # Exit a parse tree produced by X8QLParser#named_arg.
    def exitNamed_arg(self, ctx: X8QLParser.Named_argContext):
        pass

    # Enter a parse tree produced by X8QLParser#ref.
    def enterRef(self, ctx: X8QLParser.RefContext):
        pass

    # Exit a parse tree produced by X8QLParser#ref.
    def exitRef(self, ctx: X8QLParser.RefContext):
        pass

    # Enter a parse tree produced by X8QLParser#ref_path.
    def enterRef_path(self, ctx: X8QLParser.Ref_pathContext):
        pass

    # Exit a parse tree produced by X8QLParser#ref_path.
    def exitRef_path(self, ctx: X8QLParser.Ref_pathContext):
        pass

    # Enter a parse tree produced by X8QLParser#parameter.
    def enterParameter(self, ctx: X8QLParser.ParameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#parameter.
    def exitParameter(self, ctx: X8QLParser.ParameterContext):
        pass

    # Enter a parse tree produced by X8QLParser#field.
    def enterField(self, ctx: X8QLParser.FieldContext):
        pass

    # Exit a parse tree produced by X8QLParser#field.
    def exitField(self, ctx: X8QLParser.FieldContext):
        pass

    # Enter a parse tree produced by X8QLParser#field_path.
    def enterField_path(self, ctx: X8QLParser.Field_pathContext):
        pass

    # Exit a parse tree produced by X8QLParser#field_path.
    def exitField_path(self, ctx: X8QLParser.Field_pathContext):
        pass

    # Enter a parse tree produced by X8QLParser#field_primitive.
    def enterField_primitive(self, ctx: X8QLParser.Field_primitiveContext):
        pass

    # Exit a parse tree produced by X8QLParser#field_primitive.
    def exitField_primitive(self, ctx: X8QLParser.Field_primitiveContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_null.
    def enterValue_null(self, ctx: X8QLParser.Value_nullContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_null.
    def exitValue_null(self, ctx: X8QLParser.Value_nullContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_true.
    def enterValue_true(self, ctx: X8QLParser.Value_trueContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_true.
    def exitValue_true(self, ctx: X8QLParser.Value_trueContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_false.
    def enterValue_false(self, ctx: X8QLParser.Value_falseContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_false.
    def exitValue_false(self, ctx: X8QLParser.Value_falseContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_string.
    def enterValue_string(self, ctx: X8QLParser.Value_stringContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_string.
    def exitValue_string(self, ctx: X8QLParser.Value_stringContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_integer.
    def enterValue_integer(self, ctx: X8QLParser.Value_integerContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_integer.
    def exitValue_integer(self, ctx: X8QLParser.Value_integerContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_decimal.
    def enterValue_decimal(self, ctx: X8QLParser.Value_decimalContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_decimal.
    def exitValue_decimal(self, ctx: X8QLParser.Value_decimalContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_json.
    def enterValue_json(self, ctx: X8QLParser.Value_jsonContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_json.
    def exitValue_json(self, ctx: X8QLParser.Value_jsonContext):
        pass

    # Enter a parse tree produced by X8QLParser#value_array.
    def enterValue_array(self, ctx: X8QLParser.Value_arrayContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_array.
    def exitValue_array(self, ctx: X8QLParser.Value_arrayContext):
        pass

    # Enter a parse tree produced by X8QLParser#literal_string.
    def enterLiteral_string(self, ctx: X8QLParser.Literal_stringContext):
        pass

    # Exit a parse tree produced by X8QLParser#literal_string.
    def exitLiteral_string(self, ctx: X8QLParser.Literal_stringContext):
        pass

    # Enter a parse tree produced by X8QLParser#array_empty.
    def enterArray_empty(self, ctx: X8QLParser.Array_emptyContext):
        pass

    # Exit a parse tree produced by X8QLParser#array_empty.
    def exitArray_empty(self, ctx: X8QLParser.Array_emptyContext):
        pass

    # Enter a parse tree produced by X8QLParser#array_items.
    def enterArray_items(self, ctx: X8QLParser.Array_itemsContext):
        pass

    # Exit a parse tree produced by X8QLParser#array_items.
    def exitArray_items(self, ctx: X8QLParser.Array_itemsContext):
        pass

    # Enter a parse tree produced by X8QLParser#json.
    def enterJson(self, ctx: X8QLParser.JsonContext):
        pass

    # Exit a parse tree produced by X8QLParser#json.
    def exitJson(self, ctx: X8QLParser.JsonContext):
        pass

    # Enter a parse tree produced by X8QLParser#json_obj.
    def enterJson_obj(self, ctx: X8QLParser.Json_objContext):
        pass

    # Exit a parse tree produced by X8QLParser#json_obj.
    def exitJson_obj(self, ctx: X8QLParser.Json_objContext):
        pass

    # Enter a parse tree produced by X8QLParser#json_pair.
    def enterJson_pair(self, ctx: X8QLParser.Json_pairContext):
        pass

    # Exit a parse tree produced by X8QLParser#json_pair.
    def exitJson_pair(self, ctx: X8QLParser.Json_pairContext):
        pass

    # Enter a parse tree produced by X8QLParser#json_arr.
    def enterJson_arr(self, ctx: X8QLParser.Json_arrContext):
        pass

    # Exit a parse tree produced by X8QLParser#json_arr.
    def exitJson_arr(self, ctx: X8QLParser.Json_arrContext):
        pass

    # Enter a parse tree produced by X8QLParser#json_value.
    def enterJson_value(self, ctx: X8QLParser.Json_valueContext):
        pass

    # Exit a parse tree produced by X8QLParser#json_value.
    def exitJson_value(self, ctx: X8QLParser.Json_valueContext):
        pass

    # Enter a parse tree produced by X8QLParser#json_string.
    def enterJson_string(self, ctx: X8QLParser.Json_stringContext):
        pass

    # Exit a parse tree produced by X8QLParser#json_string.
    def exitJson_string(self, ctx: X8QLParser.Json_stringContext):
        pass

    # Enter a parse tree produced by X8QLParser#json_number.
    def enterJson_number(self, ctx: X8QLParser.Json_numberContext):
        pass

    # Exit a parse tree produced by X8QLParser#json_number.
    def exitJson_number(self, ctx: X8QLParser.Json_numberContext):
        pass


del X8QLParser
