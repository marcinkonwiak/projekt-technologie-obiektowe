import enum


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
