from textual._path import CSSPathType
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.widgets import Footer, Static

from src.components.database_tree import DatabaseTree
from src.settings import AppConfig


class DatabaseApp(App[None]):
    CSS_PATH = "app.tcss"
    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]
    config: AppConfig
    theme = "tokyo-night"

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

    def compose(self) -> ComposeResult:
        yield DatabaseTree(self.config.db_connections, id="db-tree")
        yield Static("TEXT \n" * 10, id="body")
        yield Footer()

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )
