from __future__ import annotations

import json
from enum import Enum
from typing import Any, Union

from x8.core.data_model import DataModel


def _str_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, dict, list)):
        return json.dumps(value)
    return str(value)


class Parameter(DataModel):
    """Parameter.

    Attributes:
        name: Parameter name.
    """

    name: str

    def __str__(self) -> str:
        return f"@{self.name}"


class Ref(DataModel):
    """Parameter.

    Attributes:
        path: Ref path.
    """

    path: str

    def __str__(self) -> str:
        return f"{{{{{self.path}}}}}"

    @staticmethod
    def parse(str: str) -> Ref:
        if str.startswith("{{") and str.endswith("}}"):
            return Ref(path=str[2:-2])
        raise ValueError("Ref format error")


class FunctionNamespace:
    """Function namespace.

    Attributes:
        BUILTIN: Builtin namespace.
    """

    BUILTIN = "builtin"


class Function(DataModel):
    """Function.

    Attributes:
        name: Function name.
        args: Function args.
        named_args: Function named args.
        namespace: Function namespace.
    """

    namespace: str = FunctionNamespace.BUILTIN
    name: str
    args: list = []
    named_args: dict = dict()

    def __str__(self) -> str:
        if self.namespace == FunctionNamespace.BUILTIN:
            str = f"{self.name}"
        else:
            str = f"{self.namespace}.{self.name}"
        str = str + self._str_args()
        return str

    def _str_args(self):
        if len(self.args) > 0:
            return f"({', '.join(_str_value(a) for a in self.args)})"
        elif len(self.named_args) > 0:
            str_args = []
            for k, v in self.named_args.items():
                str_args.append(f"{k}={_str_value(v)}")
            return f"({', '.join(a for a in str_args)})"
        else:
            return "()"


class Undefined(DataModel):
    """Undefined."""

    pass


class Field(DataModel):
    """Field.

    Attributes:
        path: Field path.
    """

    path: str

    def __str__(self) -> str:
        return self.path


class Collection(DataModel):
    """Collection.

    Attributes:
        name: Collection name.
    """

    name: str

    def __str__(self) -> str:
        return self.name


class Comparison(DataModel):
    """Comparison expression.

    Attributes:
        lexpr: Left expression.
        op: Comparison op.
        rexpr: Right expression.
    """

    lexpr: Expression
    op: ComparisonOp
    rexpr: Expression

    def __str__(self) -> str:
        str = f"{_str_value(self.lexpr)} {self.op.value} "
        if self.op == ComparisonOp.BETWEEN and isinstance(self.rexpr, list):
            str = f"""{str}{_str_value(
                self.rexpr[0])} AND {_str_value(self.rexpr[1])}"""
        elif (
            self.op == ComparisonOp.IN or self.op == ComparisonOp.NIN
        ) and isinstance(self.rexpr, list):
            str = f"""{str}({', '.join(_str_value(i) for i in self.rexpr)})"""
        else:
            str = str + _str_value(self.rexpr)
        return str

    @staticmethod
    def reverse_op(op: ComparisonOp) -> ComparisonOp:
        if op == ComparisonOp.GT:
            return ComparisonOp.LT
        if op == ComparisonOp.GTE:
            return ComparisonOp.LTE
        if op == ComparisonOp.LT:
            return ComparisonOp.GT
        if op == ComparisonOp.LTE:
            return ComparisonOp.GTE
        return op


class ComparisonOp(str, Enum):
    """Comparison op.

    Attributes:
        LT: Less than.
        LTE: Less than equals.
        GT: Greater than.
        GTE: Greater than equals.
        EQ: Equals.
        NEQ: Not equals.
        IN: In.
        NIN: Not in.
        BETWEEN: Between.
        LIKE: Like.
    """

    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    EQ = "="
    NEQ = "!="
    IN = "in"
    NIN = "not in"
    BETWEEN = "between"
    LIKE = "like"


class And(DataModel):
    """And expression.

    Attributes:
        lexpr: Left expression.
        rexpr: Right expression.
    """

    lexpr: Expression
    rexpr: Expression

    def __str__(self) -> str:
        return f"({_str_value(self.lexpr)} AND {_str_value(self.rexpr)})"


class Or(DataModel):
    """Or expression.

    Attributes:
        lexpr: Left expression.
        rexpr: Right expression.
    """

    lexpr: Expression
    rexpr: Expression

    def __str__(self) -> str:
        return f"({_str_value(self.lexpr)} OR {_str_value(self.rexpr)})"


class Not(DataModel):
    """Not expression.

    Attributes:
        expr: Expression.
    """

    expr: Expression

    def __str__(self) -> str:
        return f"NOT {_str_value(self.expr)}"


class Update(DataModel):
    """Update.

    Attributes:
        operations: Update operations.
    """

    operations: list[UpdateOperation] = []

    def put(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.PUT,
                args=[value],
            )
        )
        return self

    def insert(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.INSERT,
                args=[value],
            )
        )
        return self

    def delete(self, field: str) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.DELETE,
            )
        )
        return self

    def increment(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.INCREMENT,
                args=[value],
            )
        )
        return self

    def move(self, field: str, dest: str) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.MOVE,
                args=[dest],
            )
        )
        return self

    def array_union(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.ARRAY_UNION,
                args=[value],
            )
        )
        return self

    def array_remove(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.ARRAY_REMOVE,
                args=[value],
            )
        )
        return self

    def append(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.APPEND,
                args=[value],
            )
        )
        return self

    def prepend(self, field: str, value: Expression) -> Update:
        self.operations.append(
            UpdateOperation(
                field=field,
                op=UpdateOp.PREPEND,
                args=[value],
            )
        )
        return self

    def __str__(self) -> str:
        return f"{', '.join(f'{str(o)}' for o in self.operations)}"


class UpdateOperation(DataModel):
    """Update operation.

    Attributes:
        field: Update field.
        op: Update op.
        args: Update args.
    """

    field: str
    op: UpdateOp
    args: list = []

    def __str__(self) -> str:
        str_args = None
        if self.args is not None:
            str_args = f"{', '.join(_str_value(a) for a in self.args)}"
        if self.args is not None:
            return f"{self.field}={self.op.value}({str_args})"
        else:
            return f"{self.field}={self.op.value}()"


class UpdateOp(str, Enum):
    """Update op.

    Attributes:
        PUT:
            Added if it doesn't exist.
            Replace if it exists.
            Array element replaced at index.
        INSERT:
            Added if it doesn't exist.
            Replace if it exists.
            Array element added at index.
            Use index - to insert at the end.
        DELETE:
            Removes field.
            Array element removed at index.
        INCREMENT:
            Increment or decrement number.
            Creates field if it doesn't exist.
        MOVE:
            Move value from source to destination field.
        ARRAY_UNION:
            Union values with array without duplicates.
        ARRAY_REMOVE:
            Remove values from array
        APPEND:
            Append existing string value.
        PREPEND:
            Prepend existing string value.

    """

    PUT = "put"
    INSERT = "insert"
    DELETE = "delete"
    INCREMENT = "increment"
    MOVE = "move"
    ARRAY_UNION = "array_union"
    ARRAY_REMOVE = "array_remove"
    APPEND = "append"
    PREPEND = "prepend"


class Select(DataModel):
    """Select.

    Attributes:
        terms: Select terms.
    """

    terms: list[SelectTerm] = []

    def add_field(self, field: str, alias: str | None = None):
        self.terms.append(SelectTerm(field=field, alias=alias))

    def __str__(self) -> str:
        if len(self.terms) == 0:
            return "*"
        return f"{', '.join(str(t) for t in self.terms)}"


class SelectTerm(DataModel):
    """Select term.

    Attributes:
        field: Select field.
        alias: Field alias.
    """

    field: str
    alias: str | None

    def __str__(self) -> str:
        str = self.field
        if self.alias is not None:
            str = f"{self.field} AS {self.alias}"
        return str


class OrderBy(DataModel):
    """Order by.

    Attributes:
        terms: Order by terms.
    """

    terms: list[OrderByTerm] = []

    def add_field(
        self,
        field: str,
        direction: OrderByDirection | None = None,
    ) -> OrderBy:
        self.terms.append(OrderByTerm(field=field, direction=direction))
        return self

    def __str__(self) -> str:
        return ", ".join([str(t) for t in self.terms])


class OrderByTerm(DataModel):
    """Order by term.

    Attributes:
        field: Order by field.
        direction: Order by direction.
    """

    field: str
    direction: OrderByDirection | None = None

    def __str__(self) -> str:
        str = self.field
        if self.direction:
            str = f"{str} {self.direction.value}"
        return str


class OrderByDirection(str, Enum):
    """Order by direction.

    Attributes:
        ASC: Ascending.
        DESC: Descending.
    """

    ASC = "asc"
    DESC = "desc"


class GeoPoint(DataModel):
    """Geo point.

    Attributes:
        lat: Latitude.
        lon: Longitude.
    """

    lat: float
    lon: float

    def __str__(self) -> str:
        return f"POINT({self.lon} {self.lat})"


Value = Union[str, int, float, bool, dict, list, bytes, DataModel, None]
Expression = Union[
    Comparison, And, Or, Not, Function, Parameter, Ref, Field, Value
]
