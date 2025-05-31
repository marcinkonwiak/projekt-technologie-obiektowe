from typing import Any, Literal

from rich.style import Style
from rich.text import Text
from textual import log
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
from src.types import QueryOption, QueryOptionCondition


class TableData:
    data: list[tuple[Any, ...]]

    def __init__(
        self,
        data: list[tuple[Any, ...]] | None = None,
    ):
        self.data = data if data else []


class DatabaseTable(Widget):
    db_connection: Reactive[DBConnection | None] = reactive(None)
    table_name: Reactive[str | None] = reactive(None)
    table_metadata: TableMetadata | None = None
    table_data: Reactive[TableData] = reactive(TableData())
    displayed_columns: Reactive[list[str]] = reactive(list)

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
            self._clear_query_options()
            if self._fetch_metadata():
                self._fetch_data()

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
            log.info(
                f"watch_table_data triggered. displayed_columns are now: {self.displayed_columns}"
            )
            self._draw_table()

    def watch_displayed_columns(
        self, old_columns: list[str], new_columns: list[str]
    ) -> None:
        """Called when the displayed_columns attribute changes."""
        if self.is_mounted:
            if old_columns != new_columns:
                log(
                    f"displayed_columns changed from {old_columns} to {new_columns}. Table will be redrawn by watch_table_data if data also changed or was set."
                )
            # The elif for empty table columns might be less critical if watch_table_data handles drawing correctly.
            # Consider if any scenario needs this watcher to trigger a draw independently.
            # For now, simplifying to avoid race conditions.

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        if event.column_key.value in self.displayed_columns:
            self.order_by = event.column_key.value
            self.order_by_direction = (
                "ASC" if self.order_by_direction == "DESC" else "DESC"
            )
            self._fetch_data()
        else:
            log.warning(
                f"Order by on column '{event.column_key.value}' not possible as it's not in displayed columns."
            )

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

        cols_for_query_builder = self.table_metadata.columns

        query_composed = self.postgres_service.build_query_with_options(
            self.table_name,
            cols_for_query_builder,
            query_options=self.query_options,
            order_by_column=self.order_by,
            order_by_direction=self.order_by_direction,
        )

        data, query_string, actual_column_names = (
            self.postgres_service.get_data_from_query(query_composed)
        )
        log(f"Executed query: {query_string}")
        log(f"Returned columns: {actual_column_names}")

        self.displayed_columns = actual_column_names
        self.table_data = TableData(data=data)
        self.post_message(self.QueryUpdated(query_string))

    def _draw_table(self) -> None:
        if not self.displayed_columns:
            table = self.query_one(DataTable[str])
            table.clear()
            for column in table.columns.copy():
                table.remove_column(column)
            return

        table = self.query_one(DataTable[str])
        table.clear()

        for column in table.columns.copy():
            table.remove_column(column)

        original_metadata_map = {
            col.name: col
            for col in (self.table_metadata.columns if self.table_metadata else [])
        }

        for col_name_from_query in self.displayed_columns:
            right_padding = "  "
            if col_name_from_query == self.order_by:
                right_padding = " ↑" if self.order_by_direction == "ASC" else " ↓"

            label_text = f"{col_name_from_query}{right_padding}"

            original_col_meta = original_metadata_map.get(col_name_from_query)
            if original_col_meta and original_col_meta.is_foreign_key:
                stylized_name = Text(label_text, style=self.foreign_key_style)
            else:
                stylized_name = label_text

            table.add_column(stylized_name, key=col_name_from_query)

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
            string = f"{option.condition.to_pretty_string()} {option.column_name} "
            if option.condition == QueryOptionCondition.WHERE:
                string += f"{option.where_condition} {option.where_value}"
            elif option.condition in {
                QueryOptionCondition.LEFT_JOIN,
                QueryOptionCondition.INNER_JOIN,
            }:
                string += f"ON {option.join_to_table}.{option.join_to_column}"
            options.add_option(  # pyright: ignore [reportUnknownMemberType]
                (
                    string,
                    option_id.__str__(),
                )
            )
            option_id += 1

        options.refresh(layout=True)
        # Only fetch data if the table is properly set up with metadata
        # and the metadata corresponds to the current table_name.
        if (
            self.is_mounted
            and self.table_metadata
            and self.table_metadata.table_name == self.table_name
        ):
            self._fetch_data()

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
                where_value=result.where_value,
                where_condition=result.where_condition,
                join_to_table=result.join_to_table
                if hasattr(result, "join_to_table")
                else None,
                join_to_column=result.join_to_column
                if hasattr(result, "join_to_column")
                else None,
            )
        )
        self.mutate_reactive(DatabaseTable.query_options)

    def on_selection_list_selection_toggled(
        self, event: SelectionList.SelectionToggled[str]
    ) -> None:
        del self.query_options[event.selection_index]
        self.mutate_reactive(DatabaseTable.query_options)
        return
