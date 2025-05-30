from dataclasses import dataclass
from typing import Any, Literal

from rich.style import Style
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.reactive import Reactive, reactive
from textual.widget import Widget
from textual.widgets import DataTable, SelectionList

from src.components.add_query import AddQueryOptionModalScreenResult
from src.service.postgres import PostgresService
from src.service.types import TableMetadata
from src.settings import DBConnection
from src.types import QueryOptionCondition


class TableData:
    data: list[tuple[Any, ...]]

    def __init__(
        self,
        data: list[tuple[Any, ...]] | None = None,
    ):
        self.data = data if data else []


@dataclass
class QueryOption:
    column_name: str
    condition: QueryOptionCondition


class DatabaseTable(Widget):
    db_connection: Reactive[DBConnection | None] = reactive(None)
    table_name: Reactive[str | None] = reactive(None)
    table_metadata: TableMetadata | None = None
    table_data: Reactive[TableData] = reactive(TableData())

    query_options: Reactive[list[QueryOption]] = reactive(list)
    order_by: str | None = None
    order_by_direction: Literal["ASC", "DESC"] = "ASC"

    foreign_key_style: Style = Style(
        dim=True,
        italic=True,
    )

    def __init__(
        self,
        *,
        id: str | None = None,
    ):
        super().__init__(id=id)
        self.postgres_service = PostgresService()

    class QueryUpdated(Message):
        def __init__(self, query: str):
            super().__init__()
            self.query = query

    def compose(self) -> ComposeResult:
        yield Horizontal(
            SelectionList[str](id="query-options"),
            id="db-table-options",
        )
        yield DataTable[str](id="db-table")

    def on_mount(self) -> None:
        query_options: SelectionList[str] = self.query_one(  # pyright: ignore [reportUnknownVariableType]
            "#query-options", SelectionList
        )
        query_options.border_title = "Query Options"

        table = self.query_one(DataTable[str])
        table.zebra_stripes = True
        table.border_title = "Data"

    def watch_table_name(self, old_table_name: str | None, new_table_name: str | None):
        if self.is_mounted and self.table_name and self.db_connection:
            self._clear_filters()
            if self._fetch_metadata():
                self._fetch_data()
                self._clear_query_options()

    def watch_db_connection(
        self,
        old_db_connection: DBConnection | None,
        new_db_connection: DBConnection | None,
    ):
        if not self.is_mounted:
            return

        if old_db_connection:
            self.postgres_service.disconnect()

        if new_db_connection:
            if not self._update_db_connection():
                self.app.notify(
                    f"Failed to connect to database {new_db_connection.name}",
                    title="Connection Error",
                    severity="error",
                )

    def watch_table_data(self, old_table_data: TableData, new_table_data: TableData):
        if self.is_mounted and self.table_name and self.db_connection:
            self._draw_table()

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        self.order_by = event.column_key.value
        self.order_by_direction = "ASC" if self.order_by_direction == "DESC" else "DESC"
        self._fetch_data()

    def _fetch_metadata(self):
        assert self.db_connection
        assert self.table_name
        if not (self.postgres_service.is_connected() and self._update_db_connection()):
            self._notify_connection_error(self.db_connection.name)
            return False

        metadata = self.postgres_service.get_table_columns_metadata(self.table_name)
        self.table_metadata = metadata
        return True

    def _fetch_data(self):
        assert self.db_connection
        assert self.table_name
        assert self.table_metadata
        if not (self.postgres_service.is_connected() and self._update_db_connection()):
            self._notify_connection_error(self.db_connection.name)
            return

        data, query = self.postgres_service.get_data(
            self.table_name,
            [col.name for col in self.table_metadata.columns],
            order_by_column=self.order_by,
            order_by_direction=self.order_by_direction,
        )

        self.table_data = TableData(data=data)
        self.post_message(self.QueryUpdated(query))

    def _draw_table(self) -> None:
        assert self.table_metadata

        table = self.query_one(DataTable[str])
        table.clear()

        for column in table.columns.copy():
            table.remove_column(column)

        foreign_key_indexes: list[int] = []
        for i, column in enumerate(self.table_metadata.columns):
            right_padding = "  "
            if column.name == self.order_by:
                right_padding = " ↑" if self.order_by_direction == "ASC" else " ↓"

            n = f"{column.name}{right_padding}"
            if column.is_foreign_key:
                foreign_key_indexes.append(i)
                stylized_name = Text(n, style=self.foreign_key_style)
            else:
                stylized_name = n

            table.add_column(stylized_name, key=column.name)

        for row in self.table_data.data:
            table.add_row(*[str(r) for r in row])

    def _clear_query_options(self) -> None:
        self.query_options = []
        self.mutate_reactive(DatabaseTable.query_options)

    def watch_query_options(self) -> None:  # Added self
        options: SelectionList[str] = self.query_one("#query-options", SelectionList)  # pyright: ignore [reportUnknownVariableType]
        options.clear_options()

        option_id = 1
        for option in self.query_options:
            options.add_option(  # pyright: ignore [reportUnknownMemberType]
                (
                    f"{option.condition.to_pretty_string()} {option.column_name} ",
                    option_id.__str__(),
                )
            )
            option_id += 1

        options.refresh(layout=True)

    def _update_db_connection(self):
        assert self.db_connection

        return self.postgres_service.connect(self.db_connection)

    def _clear_filters(self) -> None:
        self.order_by = None
        self.order_by_direction = "ASC"

    def _notify_connection_error(self, database_name: str) -> None:
        self.app.notify(
            f"Failed to reconnect to database {database_name}",
            title="Connection Error",
            severity="error",
        )

    def handle_query_option(self, result: AddQueryOptionModalScreenResult) -> None:
        self.query_options.append(
            QueryOption(
                column_name=result.column_name,
                condition=result.condition,
            )
        )
        self.mutate_reactive(DatabaseTable.query_options)

    def on_selection_list_selection_toggled(
        self, event: SelectionList.SelectionToggled[str]
    ) -> None:
        del self.query_options[event.selection_index]
        self.mutate_reactive(DatabaseTable.query_options)
        return
