from typing import Any, Literal

from textual.app import ComposeResult
from textual.message import Message
from textual.reactive import Reactive, reactive
from textual.widget import Widget
from textual.widgets import DataTable

from src.service.postgres import PostgresService
from src.settings import DBConnection


class TableData:
    columns: list[str]
    data: list[tuple[Any, ...]]

    def __init__(
        self,
        columns: list[str] | None = None,
        data: list[tuple[Any, ...]] | None = None,
    ):
        self.columns = columns if columns else []
        self.data = data if data else []


class DatabaseTable(Widget):
    table_name: Reactive[str | None] = reactive(None, always_update=True)
    table_data: Reactive[TableData] = reactive(TableData(), always_update=True)
    order_by: Reactive[str | None] = Reactive(None, always_update=True)
    order_by_direction: Literal["ASC", "DESC"] = "ASC"

    def __init__(
        self,
        *,
        postgres_service: PostgresService,
        table_name: str | None = None,
        db_connection: DBConnection | None = None,
        id: str | None = None,
    ):
        super().__init__(id=id)
        self.table_name = table_name
        self.db_connection = db_connection
        self.postgres_service = postgres_service

    class QueryUpdated(Message):
        def __init__(self, query: str):
            super().__init__()
            self.query = query

    def compose(self) -> ComposeResult:
        yield DataTable[str](id="db-table")

    def on_mount(self) -> None:
        table = self.query_one(DataTable[str])
        table.zebra_stripes = True
        table.border_title = "Data"

    def watch_table_name(self, old_table_name: str | None, new_table_name: str | None):
        if self.is_mounted and self.table_name and self.db_connection:
            self._clear_filters()
            self._fetch_data()

    def watch_order_by(self, old_order_by: str | None, new_order_by: str | None):
        if self.is_mounted and self.table_name and self.db_connection:
            self._fetch_data()

    def watch_table_data(self, old_table_data: TableData, new_table_data: TableData):
        if self.is_mounted and self.table_name and self.db_connection:
            self._update_table()

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        self.order_by = event.label.plain
        self.order_by_direction = "ASC" if self.order_by_direction == "DESC" else "DESC"

    def _fetch_data(self):
        assert self.db_connection
        assert self.table_name

        if self.postgres_service.connect(self.db_connection):
            columns = self.postgres_service.get_table_columns(self.table_name)
            data, query = self.postgres_service.get_data(
                self.table_name,
                columns,
                order_by_column=self.order_by,
                order_by_direction=self.order_by_direction,
            )
            self.postgres_service.disconnect()
        else:
            self.app.notify(
                f"Failed to connect to database {self.db_connection.name}",
                title="Connection Error",
                severity="error",
            )
            return

        self.table_data = TableData(
            columns=columns,
            data=data,
        )
        self.post_message(self.QueryUpdated(query))

    def _update_table(self) -> None:
        table = self.query_one(DataTable[str])
        table.clear()

        for column in table.columns.copy():
            table.remove_column(column)

        for column in self.table_data.columns:
            table.add_column(column, key=column)

        for row in self.table_data.data:
            table.add_row(*[str(r) for r in row])

    def _clear_filters(self) -> None:
        self.order_by = None
        self.order_by_direction = "ASC"
