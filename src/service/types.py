from pydantic import BaseModel


class Column(BaseModel):
    name: str
    type: str
    is_foreign_key: bool
    foreign_key_table: str | None = None
    foreign_key_column: str | None = None


class TableMetadata(BaseModel):
    table_name: str
    columns: list[Column]
