from textual.app import ComposeResult
from textual.reactive import Reactive, reactive
from textual.widget import Widget
from textual.widgets import Tree

from src.settings import DBConnection


class DatabaseTree(Widget):
    databases: Reactive[list[DBConnection]] = reactive(list, always_update=True)

    def __init__(self, databases: list[DBConnection], id: str | None = None) -> None:
        super().__init__(id=id)
        self.databases = databases

    def compose(self) -> ComposeResult:
        yield Tree(label="Databases")

    def _clear_and_populate_tree(self) -> None:
        tree: Tree[DBConnection] = self.query_one(Tree)  # pyright: ignore [reportUnknownVariableType]
        root = tree.root

        root.remove_children()

        if self.databases:
            for db in self.databases:
                root.add(label=db.name, data=db)

    def get_selected_db_id(self) -> str | None:
        tree: Tree[DBConnection] = self.query_one(Tree)  # pyright: ignore [reportUnknownVariableType]
        cursor_node = tree.cursor_node

        if cursor_node is None or cursor_node == tree.root:
            return None

        if cursor_node.data:
            return cursor_node.data.id

        return None

    def on_mount(self) -> None:
        self._clear_and_populate_tree()

    def watch_databases(
        self, old_databases: list[DBConnection], new_databases: list[DBConnection]
    ) -> None:
        if self.is_mounted:
            self._clear_and_populate_tree()
