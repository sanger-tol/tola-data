import re

from tol.core import DataSourceFilter


class QueryParserError(Exception):
    """Error parsing a `tqc` query param"""


class QueryParser:
    """
    Parses command line query filter options for `tqc`
    """

    def __init__(self, params: list[str]) -> None:
        self.__filter_terms = {}
        if params:
            for p in params:
                self.add_param(p)

    def add_param(self, param: str) -> None:
        """Adds a filter parameter to the list of terms"""
        m = re.fullmatch(r"^([\w.]+)([=!<>%]{1,2})(.+)", param)
        if not m:
            msg = f"Failed to parse query param {param!r}"
            raise QueryParserError(msg)

        field, operator, value = m.groups()

        # ToL SDK's `and_` clause does not permit more than one filter on the
        # same field.
        if self.__filter_terms.get(field):
            msg = (
                f"More than one filter for field {field!r}\n"
                f"  Found when parsing {param!r}"
            )
            raise QueryParserError(msg)

        op, negate = self._parse_operator(operator, param)
        if value.lower() == "null" and op == "eq":
            # !=null means exists
            term = {"exists": {}} if negate is True else {"exists": {"negate": True}}
        else:
            term = (
                {op: {"value": value, "negate": True}}
                if negate is True
                else {op: {"value": value}}
            )

        self.__filter_terms[field] = term

    def filter_dict(self) -> dict[str, dict[str, str | bool]]:
        """
        Return a the filter dictionary, used in `DataSourceFilter`'s `and_` clause
        """
        return self.__filter_terms

    def data_source_filter(self) -> DataSourceFilter | None:
        terms = self.__filter_terms
        return DataSourceFilter(and_=terms) if terms else None

    def _parse_operator(self, operator: str, ctx: str) -> tuple[str, bool]:
        op = None
        negate = False
        match operator:
            case "=":
                op = "eq"
            case "!=":
                op = "eq"
                negate = True
            case "<":
                op = "lt"
            case "<=":
                op = "lte"
            case ">":
                op = "gt"
            case ">=":
                op = "gte"
            case "%":
                op = "contains"
            case "!%":
                op = "contains"
                negate = True
            case _:
                msg = f"Unknown operator {operator!r} in query param {ctx!r}"
                raise QueryParserError(msg)

        return op, negate
