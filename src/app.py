from textual._path import CSSPathType
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.widgets import Footer, Static

from src.components.add_connection_modal import (
    AddConnectionModalScreen,
    AddConnectionModalScreenResult,
)
from src.components.database_tree import DatabaseTree
from src.settings import AppConfig, DBConnection


class DatabaseApp(App[None]):
    config: AppConfig
    CSS_PATH = "css/app.tcss"
    theme = "tokyo-night"  # pyright: ignore [reportAssignmentType]
    BINDINGS = [
        ("t", "toggle_dark", "Toggle dark mode"),
        ("a", "add_db_connection", "Add database connection"),
        ("d", "remove_db_connection", "Remove selected database connection"),
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
        self.config = config

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
        yield DatabaseTree(self.config.db_connections, id="db-tree")
        yield Static("---\n" * 10, id="body")
        yield Footer()

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )

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
