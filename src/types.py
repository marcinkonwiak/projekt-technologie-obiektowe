import enum
from dataclasses import dataclass


class QueryOptionCondition(enum.Enum):
    LEFT_JOIN = "left_join"
    INNER_JOIN = "inner_join"
    WHERE = "where"
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MAX = "max"
    MIN = "min"

    def to_pretty_string(self) -> str:
        return self.value.upper().replace("_", " ")


@dataclass
class QueryOption:
    column_name: str
    condition: QueryOptionCondition
    where_condition: str | None = None
    where_value: str | None = None
    join_to_table: str | None = None
    join_to_column: str | None = None
