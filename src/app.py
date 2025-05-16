from textual._path import CSSPathType  # noqa
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.widgets import Footer

from src.components.add_connection_modal import (
    AddConnectionModalScreen,
    AddConnectionModalScreenResult,
)
from src.components.database_table import DatabaseTable
from src.components.database_tree import DatabaseTree
from src.service.postgres import PostgresService
from src.settings import AppConfig, DBConnection


class DatabaseApp(App[None]):
    config: AppConfig
    CSS_PATH = "css/app.tcss"
    BINDINGS = [
        ("A", "add_db_connection", "Add database connection"),
        ("D", "remove_db_connection", "Remove selected database connection"),
    ]

    def __init__(
        self,
        config: AppConfig,
        driver_class: type[Driver] | None = None,
        css_path: CSSPathType | None = None,
        watch_css: bool = False,
        ansi_color: bool = False,
    ):
        super().__init__(driver_class, css_path, watch_css, ansi_color)
        self.theme = "tokyo-night"
        self.config = config
        self.postgres_service = PostgresService()

    def action_add_db_connection(self) -> None:
        def check_result(result: AddConnectionModalScreenResult | None):
            if not result:
                return

            connection = DBConnection(
                name=result.name,
                host=result.host,
                port=result.port,
                user=result.user,
                password=result.password,
                database=result.database,
            )
            self.config.add_db_connection(connection)
            self.notify(
                "Database connection added successfully",
                title="Success",
                timeout=3,
            )

            self.query_one(DatabaseTree).databases = self.config.db_connections

        self.push_screen(AddConnectionModalScreen(), check_result)

    def compose(self) -> ComposeResult:
        yield DatabaseTree(
            postgres_service=self.postgres_service,
            databases=self.config.db_connections,
            id="db-tree",
        )
        yield DatabaseTable(
            postgres_service=self.postgres_service,
            id="db-table",
        )
        yield Footer()

    def on_database_tree_table_selected(
        self, event: DatabaseTree.TableSelected
    ) -> None:
        table = self.query_one(DatabaseTable)
        table.db_connection = event.connection
        table.table_name = event.table

    def action_remove_db_connection(self) -> None:
        db_tree = self.query_one(DatabaseTree)
        selected_id = db_tree.get_selected_db_id()

        if selected_id is None:
            self.notify(
                "No database connection selected",
                title="Error",
                severity="error",
                timeout=3,
            )
            return

        connection = None
        for conn in self.config.db_connections:
            if conn.id == selected_id:
                connection = conn
                break

        if connection and self.config.remove_db_connection(selected_id):
            self.notify(
                f"Database connection '{connection.name}' removed",
                title="Success",
                timeout=3,
            )

            db_tree.databases = self.config.db_connections
