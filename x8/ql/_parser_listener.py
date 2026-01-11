# flake8: noqa
# type: ignore
import decimal
import json

from antlr4 import *

from x8.core._operation import Operation

from ._models import (
    And,
    Collection,
    Comparison,
    ComparisonOp,
    Field,
    Function,
    Not,
    Or,
    OrderBy,
    OrderByTerm,
    Parameter,
    Ref,
    Select,
    SelectTerm,
    Update,
    UpdateOperation,
)
from .generated.X8QLParser import X8QLParser


# This class defines a complete listener for a parse tree produced by X8QLParser.
class X8QLParserListener(ParseTreeListener):
    operation = None
    select = None
    collection = None
    where = None
    order_by = None
    update = None
    search = None
    rank_by = None

    # Enter a parse tree produced by X8QLParser#identifier.
    def enterIdentifier(self, ctx: X8QLParser.IdentifierContext):
        pass

    # Exit a parse tree produced by X8QLParser#identifier.
    def exitIdentifier(self, ctx: X8QLParser.IdentifierContext):
        ctx.text = ctx.getText()

    # Enter a parse tree produced by X8QLParser#parse_statement.
    def enterParse_statement(self, ctx: X8QLParser.Parse_statementContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_statement.
    def exitParse_statement(self, ctx: X8QLParser.Parse_statementContext):
        self.operation = ctx.statement().val

    # Enter a parse tree produced by X8QLParser#parse_search.
    def enterParse_search(self, ctx: X8QLParser.Parse_searchContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_search.
    def exitParse_search(self, ctx: X8QLParser.Parse_searchContext):
        self.search = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#parse_where.
    def enterParse_where(self, ctx: X8QLParser.Parse_whereContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_where.
    def exitParse_where(self, ctx: X8QLParser.Parse_whereContext):
        self.where = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#parse_select.
    def enterParse_select(self, ctx: X8QLParser.Parse_selectContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_select.
    def exitParse_select(self, ctx: X8QLParser.Parse_selectContext):
        self.select = ctx.select().val

    # Enter a parse tree produced by X8QLParser#parse_collection.
    def enterParse_collection(self, ctx: X8QLParser.Parse_collectionContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_collection.
    def exitParse_collection(self, ctx: X8QLParser.Parse_collectionContext):
        self.collection = ctx.collection().val

    # Enter a parse tree produced by X8QLParser#parse_order_by.
    def enterParse_order_by(self, ctx: X8QLParser.Parse_order_byContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_order_by.
    def exitParse_order_by(self, ctx: X8QLParser.Parse_order_byContext):
        self.order_by = ctx.order_by().val

    # Enter a parse tree produced by X8QLParser#parse_rank_by.
    def enterParse_rank_by(self, ctx: X8QLParser.Parse_rank_byContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_rank_by.
    def exitParse_rank_by(self, ctx: X8QLParser.Parse_rank_byContext):
        self.rank_by = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#parse_update.
    def enterParse_update(self, ctx: X8QLParser.Parse_updateContext):
        pass

    # Exit a parse tree produced by X8QLParser#parse_update.
    def exitParse_update(self, ctx: X8QLParser.Parse_updateContext):
        self.update = ctx.update().val

    # Enter a parse tree produced by X8QLParser#statement_single.
    def enterStatement_single(self, ctx: X8QLParser.Statement_singleContext):
        pass

    # Exit a parse tree produced by X8QLParser#statement_single.
    def exitStatement_single(self, ctx: X8QLParser.Statement_singleContext):
        op = ctx.op.text.lower()
        args = dict()
        for clause in ctx.clause():
            args[clause.key] = clause.val
        ctx.val = Operation(name=op, args=args)

    # Enter a parse tree produced by X8QLParser#statement_multi.
    def enterStatement_multi(self, ctx: X8QLParser.Statement_multiContext):
        pass

    # Exit a parse tree produced by X8QLParser#statement_multi.
    def exitStatement_multi(self, ctx: X8QLParser.Statement_multiContext):
        op = ctx.op.text.lower()
        operations = []
        for statement in ctx.statement():
            operations.append(statement.val)
        if op == "batch":
            arg = "batch"
        elif op == "transact":
            arg = "transaction"
        else:
            arg = op
        ctx.val = Operation(name=op, args={arg: {"operations": operations}})

    # Enter a parse tree produced by X8QLParser#clause.
    def enterClause(self, ctx: X8QLParser.ClauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#clause.
    def exitClause(self, ctx: X8QLParser.ClauseContext):
        clause = None
        if ctx.select_clause() is not None:
            clause = ctx.select_clause()
        elif ctx.collection_clause() is not None:
            clause = ctx.collection_clause()
        elif ctx.set_clause() is not None:
            clause = ctx.set_clause()
        elif ctx.where_clause() is not None:
            clause = ctx.where_clause()
        elif ctx.order_by_clause() is not None:
            clause = ctx.order_by_clause()
        elif ctx.rank_by_clause() is not None:
            clause = ctx.rank_by_clause()
        elif ctx.search_clause() is not None:
            clause = ctx.search_clause()
        elif ctx.generic_clause() is not None:
            clause = ctx.generic_clause()
        ctx.key = clause.key
        ctx.val = clause.val

    # Enter a parse tree produced by X8QLParser#generic_clause.
    def enterGeneric_clause(self, ctx: X8QLParser.Generic_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#generic_clause.
    def exitGeneric_clause(self, ctx: X8QLParser.Generic_clauseContext):
        ctx.key = ctx.name.text.lower()
        ctx.val = ctx.operand().val

    # Enter a parse tree produced by X8QLParser#select_clause.
    def enterSelect_clause(self, ctx: X8QLParser.Select_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_clause.
    def exitSelect_clause(self, ctx: X8QLParser.Select_clauseContext):
        ctx.key = "select"
        ctx.val = ctx.select().val

    # Enter a parse tree produced by X8QLParser#select_all.
    def enterSelect_all(self, ctx: X8QLParser.Select_allContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_all.
    def exitSelect_all(self, ctx: X8QLParser.Select_allContext):
        ctx.val = Select()

    # Enter a parse tree produced by X8QLParser#select_terms.
    def enterSelect_terms(self, ctx: X8QLParser.Select_termsContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_terms.
    def exitSelect_terms(self, ctx: X8QLParser.Select_termsContext):
        ctx.val = Select(terms=[item.val for item in ctx.select_term()])

    # Enter a parse tree produced by X8QLParser#select_parameter.
    def enterSelect_parameter(self, ctx: X8QLParser.Select_parameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_parameter.
    def exitSelect_parameter(self, ctx: X8QLParser.Select_parameterContext):
        ctx.val = ctx.parameter().val

    # Enter a parse tree produced by X8QLParser#select_term.
    def enterSelect_term(self, ctx: X8QLParser.Select_termContext):
        pass

    # Exit a parse tree produced by X8QLParser#select_term.
    def exitSelect_term(self, ctx: X8QLParser.Select_termContext):
        ctx.val = SelectTerm(
            field=ctx.field()[0].val.path,
            alias=None if len(ctx.field()) == 1 else ctx.field()[1].val.path,
        )

    # Enter a parse tree produced by X8QLParser#collection_clause.
    def enterCollection_clause(self, ctx: X8QLParser.Collection_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#collection_clause.
    def exitCollection_clause(self, ctx: X8QLParser.Collection_clauseContext):
        ctx.key = "collection"
        ctx.val = ctx.collection().val

    # Enter a parse tree produced by X8QLParser#collection_identifier.
    def enterCollection_identifier(
        self, ctx: X8QLParser.Collection_identifierContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#collection_identifier.
    def exitCollection_identifier(
        self, ctx: X8QLParser.Collection_identifierContext
    ):
        ctx.val = Collection(name=ctx.getText())

    # Enter a parse tree produced by X8QLParser#collection_parameter.
    def enterCollection_parameter(
        self, ctx: X8QLParser.Collection_parameterContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#collection_parameter.
    def exitCollection_parameter(
        self, ctx: X8QLParser.Collection_parameterContext
    ):
        ctx.val = ctx.parameter().val

    # Enter a parse tree produced by X8QLParser#search_clause.
    def enterSearch_clause(self, ctx: X8QLParser.Search_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#search_clause.
    def exitSearch_clause(self, ctx: X8QLParser.Search_clauseContext):
        ctx.key = "search"
        ctx.val = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#where_clause.
    def enterWhere_clause(self, ctx: X8QLParser.Where_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#where_clause.
    def exitWhere_clause(self, ctx: X8QLParser.Where_clauseContext):
        ctx.key = "where"
        ctx.val = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#expression_operand.
    def enterExpression_operand(
        self, ctx: X8QLParser.Expression_operandContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_operand.
    def exitExpression_operand(
        self, ctx: X8QLParser.Expression_operandContext
    ):
        ctx.val = ctx.operand().val

    # Enter a parse tree produced by X8QLParser#expression_paranthesis.
    def enterExpression_paranthesis(
        self, ctx: X8QLParser.Expression_paranthesisContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_paranthesis.
    def exitExpression_paranthesis(
        self, ctx: X8QLParser.Expression_paranthesisContext
    ):
        ctx.val = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#expression_comparison.
    def enterExpression_comparison(
        self, ctx: X8QLParser.Expression_comparisonContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_comparison.
    def exitExpression_comparison(
        self, ctx: X8QLParser.Expression_comparisonContext
    ):
        ctx.val = Comparison(
            lexpr=ctx.lhs.val,
            op=ctx.op.text.lower(),
            rexpr=ctx.rhs.val,
        )

    # Enter a parse tree produced by X8QLParser#expression_comparison_between.
    def enterExpression_comparison_between(
        self, ctx: X8QLParser.Expression_comparison_betweenContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_comparison_between.
    def exitExpression_comparison_between(
        self, ctx: X8QLParser.Expression_comparison_betweenContext
    ):
        ctx.val = Comparison(
            lexpr=ctx.lhs.val,
            op=ComparisonOp.BETWEEN,
            rexpr=[ctx.low.val, ctx.high.val],
        )

    # Enter a parse tree produced by X8QLParser#expression_comparison_in.
    def enterExpression_comparison_in(
        self, ctx: X8QLParser.Expression_comparison_inContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#expression_comparison_in.
    def exitExpression_comparison_in(
        self, ctx: X8QLParser.Expression_comparison_inContext
    ):
        op = ComparisonOp.IN if ctx.not_in is None else ComparisonOp.NIN
        args = ctx.operand()
        ctx.val = Comparison(
            lexpr=ctx.lhs.val,
            op=op,
            rexpr=[args[i].val for i in range(1, len(args))],
        )

    # Enter a parse tree produced by X8QLParser#expression_not.
    def enterExpression_not(self, ctx: X8QLParser.Expression_notContext):
        pass

    # Exit a parse tree produced by X8QLParser#expression_not.
    def exitExpression_not(self, ctx: X8QLParser.Expression_notContext):
        ctx.val = Not(expr=ctx.expression().val)

    # Enter a parse tree produced by X8QLParser#expression_and.
    def enterExpression_and(self, ctx: X8QLParser.Expression_andContext):
        pass

    # Exit a parse tree produced by X8QLParser#expression_and.
    def exitExpression_and(self, ctx: X8QLParser.Expression_andContext):
        ctx.val = And(lexpr=ctx.lhs.val, rexpr=ctx.rhs.val)

    # Enter a parse tree produced by X8QLParser#expression_or.
    def enterExpression_or(self, ctx: X8QLParser.Expression_orContext):
        pass

    # Exit a parse tree produced by X8QLParser#expression_or.
    def exitExpression_or(self, ctx: X8QLParser.Expression_orContext):
        ctx.val = Or(lexpr=ctx.lhs.val, rexpr=ctx.rhs.val)

        # Enter a parse tree produced by X8QLParser#operand_value.

    def enterOperand_value(self, ctx: X8QLParser.Operand_valueContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_value.
    def exitOperand_value(self, ctx: X8QLParser.Operand_valueContext):
        ctx.val = ctx.value().val

    # Enter a parse tree produced by X8QLParser#operand_field.
    def enterOperand_field(self, ctx: X8QLParser.Operand_fieldContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_field.
    def exitOperand_field(self, ctx: X8QLParser.Operand_fieldContext):
        ctx.val = ctx.field().val

    # Enter a parse tree produced by X8QLParser#operand_parameter.
    def enterOperand_parameter(self, ctx: X8QLParser.Operand_parameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_parameter.
    def exitOperand_parameter(self, ctx: X8QLParser.Operand_parameterContext):
        ctx.val = ctx.parameter().val

    # Enter a parse tree produced by X8QLParser#operand_ref.
    def enterOperand_ref(self, ctx: X8QLParser.Operand_refContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_ref.
    def exitOperand_ref(self, ctx: X8QLParser.Operand_refContext):
        ctx.val = ctx.ref().val

    # Enter a parse tree produced by X8QLParser#operand_function.
    def enterOperand_function(self, ctx: X8QLParser.Operand_functionContext):
        pass

    # Exit a parse tree produced by X8QLParser#operand_function.
    def exitOperand_function(self, ctx: X8QLParser.Operand_functionContext):
        ctx.val = ctx.function().val

    # Enter a parse tree produced by X8QLParser#order_by_clause.
    def enterOrder_by_clause(self, ctx: X8QLParser.Order_by_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_clause.
    def exitOrder_by_clause(self, ctx: X8QLParser.Order_by_clauseContext):
        ctx.key = "order_by"
        ctx.val = ctx.order_by().val

    # Enter a parse tree produced by X8QLParser#rank_by_clause.
    def enterRank_by_clause(self, ctx: X8QLParser.Rank_by_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#rank_by_clause.
    def exitRank_by_clause(self, ctx: X8QLParser.Rank_by_clauseContext):
        ctx.key = "rank_by"
        ctx.val = ctx.expression().val

    # Enter a parse tree produced by X8QLParser#order_by_terms.
    def enterOrder_by_terms(self, ctx: X8QLParser.Order_by_termsContext):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_terms.
    def exitOrder_by_terms(self, ctx: X8QLParser.Order_by_termsContext):
        ctx.val = OrderBy(terms=[term.val for term in ctx.order_by_term()])

    # Enter a parse tree produced by X8QLParser#order_by_parameter.
    def enterOrder_by_parameter(
        self, ctx: X8QLParser.Order_by_parameterContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_parameter.
    def exitOrder_by_parameter(
        self, ctx: X8QLParser.Order_by_parameterContext
    ):
        ctx.val = ctx.parameter().val

    # Enter a parse tree produced by X8QLParser#order_by_term.
    def enterOrder_by_term(self, ctx: X8QLParser.Order_by_termContext):
        pass

    # Exit a parse tree produced by X8QLParser#order_by_term.
    def exitOrder_by_term(self, ctx: X8QLParser.Order_by_termContext):
        ctx.val = OrderByTerm(
            field=ctx.field().val.path,
            direction=(
                None if ctx.direction is None else ctx.direction.text.lower()
            ),
        )

    # Enter a parse tree produced by X8QLParser#set_clause.
    def enterSet_clause(self, ctx: X8QLParser.Set_clauseContext):
        pass

    # Exit a parse tree produced by X8QLParser#set_clause.
    def exitSet_clause(self, ctx: X8QLParser.Set_clauseContext):
        ctx.key = "set"
        ctx.val = ctx.update().val

    # Enter a parse tree produced by X8QLParser#update_operations.
    def enterUpdate_operations(self, ctx: X8QLParser.Update_operationsContext):
        pass

    # Exit a parse tree produced by X8QLParser#update_operations.
    def exitUpdate_operations(self, ctx: X8QLParser.Update_operationsContext):
        ctx.val = Update(operations=[op.val for op in ctx.update_operation()])

    # Enter a parse tree produced by X8QLParser#update_parameter.
    def enterUpdate_parameter(self, ctx: X8QLParser.Update_parameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#update_parameter.
    def exitUpdate_parameter(self, ctx: X8QLParser.Update_parameterContext):
        ctx.val = ctx.parameter().val

    # Enter a parse tree produced by X8QLParser#update_operation.
    def enterUpdate_operation(self, ctx: X8QLParser.Update_operationContext):
        pass

    # Exit a parse tree produced by X8QLParser#update_operation.
    def exitUpdate_operation(self, ctx: X8QLParser.Update_operationContext):
        args = ctx.function().val.args
        if args is None:
            ctx.val = UpdateOperation(
                field=ctx.field().val.path,
                op=ctx.function().val.name.lower(),
            )
        else:
            ctx.val = UpdateOperation(
                field=ctx.field().val.path,
                op=ctx.function().val.name.lower(),
                args=args,
            )

    # Enter a parse tree produced by X8QLParser#function.
    def enterFunction(self, ctx: X8QLParser.FunctionContext):
        pass

    # Exit a parse tree produced by X8QLParser#function.
    def exitFunction(self, ctx: X8QLParser.FunctionContext):
        args = ctx.function_args()
        if ctx.namespace is None:
            if isinstance(args.val, list):
                ctx.val = Function(name=ctx.name.text.lower(), args=args.val)
            elif isinstance(args.val, dict):
                ctx.val = Function(
                    name=ctx.name.text.lower(), named_args=args.val
                )
            else:
                ctx.val = Function(name=ctx.name.text.lower())
        else:
            if isinstance(args.val, list):
                ctx.val = Function(
                    name=ctx.name.text.lower(),
                    args=args.val,
                    namespace=ctx.namespace.text.lower(),
                )
            elif isinstance(args, dict):
                ctx.val = Function(
                    name=ctx.name.text.lower(),
                    named_args=args.val,
                    namespace=ctx.namespace.text.lower(),
                )
            else:
                ctx.val = Function(
                    ctx.name.text.lower(),
                    ctx.namespace.text.lower(),
                )

    # Enter a parse tree produced by X8QLParser#function_no_args.
    def enterFunction_no_args(self, ctx: X8QLParser.Function_no_argsContext):
        pass

    # Exit a parse tree produced by X8QLParser#function_no_args.
    def exitFunction_no_args(self, ctx: X8QLParser.Function_no_argsContext):
        ctx.val = None

    # Enter a parse tree produced by X8QLParser#function_with_args.
    def enterFunction_with_args(
        self, ctx: X8QLParser.Function_with_argsContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#function_with_args.
    def exitFunction_with_args(
        self, ctx: X8QLParser.Function_with_argsContext
    ):
        ctx.val = [expr.val for expr in ctx.operand()]

    # Enter a parse tree produced by X8QLParser#function_with_named_args.
    def enterFunction_with_named_args(
        self, ctx: X8QLParser.Function_with_named_argsContext
    ):
        pass

    # Exit a parse tree produced by X8QLParser#function_with_named_args.
    def exitFunction_with_named_args(
        self, ctx: X8QLParser.Function_with_named_argsContext
    ):
        kwargs = dict()
        for arg in ctx.named_arg():
            kwargs[arg.val[0]] = arg.val[1]
        ctx.val = kwargs

    # Enter a parse tree produced by X8QLParser#named_arg.
    def enterNamed_arg(self, ctx: X8QLParser.Named_argContext):
        pass

    # Exit a parse tree produced by X8QLParser#named_arg.
    def exitNamed_arg(self, ctx: X8QLParser.Named_argContext):
        ctx.val = (ctx.name.text, ctx.operand().val)

    # Enter a parse tree produced by X8QLParser#ref.
    def enterRef(self, ctx: X8QLParser.RefContext):
        pass

    # Exit a parse tree produced by X8QLParser#ref.
    def exitRef(self, ctx: X8QLParser.RefContext):
        ctx.val = Ref(path=ctx.path)

    # Enter a parse tree produced by X8QLParser#parameter.
    def enterParameter(self, ctx: X8QLParser.ParameterContext):
        pass

    # Exit a parse tree produced by X8QLParser#parameter.
    def exitParameter(self, ctx: X8QLParser.ParameterContext):
        ctx.val = Parameter(name=ctx.name.text)

    # Enter a parse tree produced by X8QLParser#field.
    def enterField(self, ctx: X8QLParser.FieldContext):
        pass

    # Exit a parse tree produced by X8QLParser#field.
    def exitField(self, ctx: X8QLParser.FieldContext):
        ctx.val = Field(path=ctx.getText())

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
        ctx.val = None

    # Enter a parse tree produced by X8QLParser#value_true.
    def enterValue_true(self, ctx: X8QLParser.Value_trueContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_true.
    def exitValue_true(self, ctx: X8QLParser.Value_trueContext):
        ctx.val = True

    # Enter a parse tree produced by X8QLParser#value_false.
    def enterValue_false(self, ctx: X8QLParser.Value_falseContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_false.
    def exitValue_false(self, ctx: X8QLParser.Value_falseContext):
        ctx.val = False

    # Enter a parse tree produced by X8QLParser#value_string.
    def enterValue_string(self, ctx: X8QLParser.Value_stringContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_string.
    def exitValue_string(self, ctx: X8QLParser.Value_stringContext):
        ctx.val = eval(ctx.getText())

    # Enter a parse tree produced by X8QLParser#value_integer.
    def enterValue_integer(self, ctx: X8QLParser.Value_integerContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_integer.
    def exitValue_integer(self, ctx: X8QLParser.Value_integerContext):
        ctx.val = int(ctx.getText())

    # Enter a parse tree produced by X8QLParser#value_decimal.
    def enterValue_decimal(self, ctx: X8QLParser.Value_decimalContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_decimal.
    def exitValue_decimal(self, ctx: X8QLParser.Value_decimalContext):
        ctx.val = float(decimal.Decimal(ctx.getText()))

    # Enter a parse tree produced by X8QLParser#value_json.
    def enterValue_json(self, ctx: X8QLParser.Value_jsonContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_json.
    def exitValue_json(self, ctx: X8QLParser.Value_jsonContext):
        ctx.val = json.loads(ctx.getText())

    # Enter a parse tree produced by X8QLParser#value_array.
    def enterValue_array(self, ctx: X8QLParser.Value_arrayContext):
        pass

    # Exit a parse tree produced by X8QLParser#value_array.
    def exitValue_array(self, ctx: X8QLParser.Value_arrayContext):
        ctx.val = ctx.array().val

    # Enter a parse tree produced by X8QLParser#array_empty.
    def enterArray_empty(self, ctx: X8QLParser.Array_emptyContext):
        pass

    # Exit a parse tree produced by X8QLParser#array_empty.
    def exitArray_empty(self, ctx: X8QLParser.Array_emptyContext):
        ctx.val = []

    # Enter a parse tree produced by X8QLParser#array_items.
    def enterArray_items(self, ctx: X8QLParser.Array_itemsContext):
        pass

    # Exit a parse tree produced by X8QLParser#array_items.
    def exitArray_items(self, ctx: X8QLParser.Array_itemsContext):
        ctx.val = [item.val for item in ctx.value()]

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


del X8QLParser
