from textual._path import CSSPathType
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.widgets import Footer, Static

from src.components.add_connection_modal import AddConnectionModalScreen
from src.components.database_tree import DatabaseTree
from src.settings import AppConfig


class DatabaseApp(App[None]):
    config: AppConfig
    CSS_PATH = "css/app.tcss"
    theme = "tokyo-night"
    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("a", "add_db_connection", "Add database connection"),
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
        self.push_screen(AddConnectionModalScreen())

    def compose(self) -> ComposeResult:
        yield DatabaseTree(self.config.db_connections, id="db-tree")
        yield Static("TEXT \n" * 10, id="body")
        yield Footer()

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )
