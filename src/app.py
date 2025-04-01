from textual.app import App, ComposeResult
from textual.widgets import Footer, Header

from src.components.database_tree import DatabaseTree


class DatabaseApp(App[None]):
    CSS_PATH = "app.tcss"
    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield DatabaseTree(label="Database")
        yield Footer()

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )
