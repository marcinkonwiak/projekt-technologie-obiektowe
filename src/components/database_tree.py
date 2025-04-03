from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import Tree

from src.settings import DBConnection


class DatabaseTree(Widget):
    databases: list[DBConnection]

    def __init__(self, databases: list[DBConnection], id=None) -> None:
        self.databases = databases
        super().__init__(id=id)

    def compose(self) -> ComposeResult:
        yield Tree(label="Databases")

    def on_mount(self) -> None:
        tree = self.query_one(Tree)
        for db in self.databases:
            tree.root.add(label=db.name, data=db)
