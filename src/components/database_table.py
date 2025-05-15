from textual.app import ComposeResult
from textual.reactive import Reactive, reactive
from textual.widget import Widget
from textual.widgets import DataTable

from src.service.postgres import PostgresService
from src.settings import DBConnection


class DatabaseTable(Widget):
    table: Reactive[str | None] = reactive(None, always_update=True)

    def __init__(
        self,
        *,
        postgres_service: PostgresService,
        table: str | None = None,
        db_connection: DBConnection | None = None,
        id: str | None = None,
    ):
        super().__init__(id=id)
        self.table = table
        self.db_connection = db_connection
        self.postgres_service = postgres_service

    def compose(self) -> ComposeResult:
        yield DataTable[str](id="data-table")

    def populate_table(self) -> None:
        table = self.query_one(DataTable[str])
        assert self.db_connection
        assert self.table

        try:
            if self.postgres_service.connect(self.db_connection):
                columns = self.postgres_service.get_table_columns(self.table)
                data = self.postgres_service.get_data(self.table, columns)
                self.postgres_service.disconnect()
            else:
                self.app.notify(
                    f"Failed to connect to database {self.db_connection.name}",
                    title="Connection Error",
                    severity="error",
                )
                return
        except Exception as e:
            self.app.notify(
                f"Error fetching data from {self.table}: {e}",
                title="Error",
                severity="error",
            )
            return

        for column in columns:
            table.add_column(column, key=column)

        for row in data:
            table.add_row(*[str(r) for r in row])

    def clear_table(self) -> None:
        table = self.query_one(DataTable[str])
        table.clear()
        columns = table.columns.copy()
        for column in columns:
            table.remove_column(column)

    def watch_table(self, old_table: str | None, new_table: str | None):
        if self.is_mounted and self.table and self.db_connection:
            self.clear_table()
            self.populate_table()
