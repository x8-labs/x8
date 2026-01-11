import copy
from functools import lru_cache
from typing import Any

from antlr4 import CommonTokenStream  # type: ignore
from antlr4 import InputStream  # type: ignore
from antlr4 import ParseTreeWalker  # type: ignore
from antlr4.error.ErrorListener import ConsoleErrorListener, ErrorListener

from x8.core._operation import Operation

from ._models import Collection, Expression, OrderBy, Select, Update
from ._parser_listener import X8QLParserListener  # type: ignore
from .exceptions import ParserError
from .generated.X8QLLexer import X8QLLexer  # type: ignore
from .generated.X8QLParser import X8QLParser  # type: ignore


class QLParser:
    @lru_cache
    @staticmethod
    def parse(str: str, type: str) -> Any:
        obj = None
        if str is None:
            return None
        parser = QLParser._get_parser(str=str)
        if type == "statement":
            tree = parser.parse_statement()
            listener = QLParser._get_listener(tree)
            obj = listener.operation
        elif type == "where":
            tree = parser.parse_where()
            listener = QLParser._get_listener(tree)
            obj = listener.where
        elif type == "select":
            tree = parser.parse_select()
            listener = QLParser._get_listener(tree)
            obj = listener.select
        elif type == "collection":
            tree = parser.parse_collection()
            listener = QLParser._get_listener(tree)
            obj = listener.collection
        elif type == "order_by":
            tree = parser.parse_order_by()
            listener = QLParser._get_listener(tree)
            obj = listener.order_by
        elif type == "rank_by":
            tree = parser.parse_rank_by()
            listener = QLParser._get_listener(tree)
            obj = listener.rank_by
        elif type == "search":
            tree = parser.parse_search()
            listener = QLParser._get_listener(tree)
            obj = listener.search
        elif type == "update":
            tree = parser.parse_update()
            listener = QLParser._get_listener(tree)
            obj = listener.update
        return obj

    @staticmethod
    def parse_statement(str: str) -> Operation | None:
        return copy.deepcopy(QLParser.parse(str, "statement"))

    @staticmethod
    def parse_where(str: str) -> Expression:
        return copy.deepcopy(QLParser.parse(str, "where"))

    @staticmethod
    def parse_select(str: str) -> Select | None:
        return copy.deepcopy(QLParser.parse(str, "select"))

    @staticmethod
    def parse_collection(str: str) -> Collection | None:
        return copy.deepcopy(QLParser.parse(str, "collection"))

    @staticmethod
    def parse_order_by(str: str) -> OrderBy | None:
        return copy.deepcopy(QLParser.parse(str, "order_by"))

    @staticmethod
    def parse_rank_by(str: str) -> Expression | None:
        return copy.deepcopy(QLParser.parse(str, "rank_by"))

    @staticmethod
    def parse_search(str: str) -> Expression:
        return copy.deepcopy(QLParser.parse(str, "search"))

    @staticmethod
    def parse_update(str: str) -> Update | None:
        return copy.deepcopy(QLParser.parse(str, "update"))

    @staticmethod
    def _get_listener(tree) -> X8QLParserListener:
        listener = X8QLParserListener()
        walker = ParseTreeWalker()
        walker.walk(listener, tree)
        return listener

    @staticmethod
    def _get_parser(str: str) -> X8QLParser:
        input_stream = InputStream(str)
        lexer = X8QLLexer(input_stream)
        stream = CommonTokenStream(lexer)
        parser = X8QLParser(stream)
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE)
        parser.addErrorListener(ParserErrorListener())
        return parser


class ParserErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise ParserError("line " + str(line) + ":" + str(column) + " " + msg)

    def reportAmbiguity(
        self, recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs
    ):
        pass

    def reportAttemptingFullContext(
        self, recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs
    ):
        pass

    def reportContextSensitivity(
        self, recognizer, dfa, startIndex, stopIndex, prediction, configs
    ):
        pass
