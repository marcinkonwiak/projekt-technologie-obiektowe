from typing import Any

from textual.app import ComposeResult
from textual.message import Message
from textual.reactive import Reactive, reactive
from textual.widget import Widget
from textual.widgets import Tree

from src.service.postgres import PostgresService
from src.settings import DBConnection


class DatabaseTree(Widget):
    databases: Reactive[list[DBConnection]] = reactive(list, always_update=True)
    postgres_service: PostgresService

    def __init__(
        self,
        postgres_service: PostgresService,
        databases: list[DBConnection],
        id: str | None = None,
    ) -> None:
        super().__init__(id=id)
        self.databases = databases
        self.postgres_service = postgres_service

    class Table:
        def __init__(self, name: str) -> None:
            self.name = name

    class TableSelected(Message):
        def __init__(self, connection: DBConnection, table: str):
            super().__init__()
            self.connection = connection
            self.table = table

    def compose(self) -> ComposeResult:
        yield Tree(label="Databases")

    def on_mount(self) -> None:
        self._clear_and_populate_tree()

    def watch_databases(
        self, old_databases: list[DBConnection], new_databases: list[DBConnection]
    ) -> None:
        if self.is_mounted:
            self._clear_and_populate_tree()

    def _clear_and_populate_tree(self) -> None:
        tree: Tree[DBConnection] = self.query_one(Tree)  # pyright: ignore [reportUnknownVariableType]
        root = tree.root

        root.remove_children()

        if self.databases:
            for db in self.databases:
                node = root.add(label=db.name, data=db, expand=False)
                node.add_leaf("Loading tables...")

    def on_tree_node_selected(self, event: Tree.NodeSelected[Any]) -> None:
        node = event.node

        if node == self.query_one(Tree[DBConnection]).root or not node.data:
            return

        if (
            isinstance(node.data, self.Table)
            and node.parent
            and node.parent.data
            and isinstance(node.parent.data, DBConnection)
        ):
            self.post_message(
                self.TableSelected(
                    connection=node.parent.data,
                    table=node.data.name,
                )
            )

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded[Any]) -> None:
        node = event.node
        node_data = node.data

        if (
            node == self.query_one(Tree[DBConnection]).root
            or not node_data
            or not isinstance(node_data, DBConnection)
        ):
            return

        node.remove_children()

        assert isinstance(node_data, DBConnection)
        worker = self.run_worker(
            lambda: self._fetch_tables(node_data),
            thread=True,
            group="db_operations",
        )
        await worker.wait()

        tables = worker.result
        if tables:
            for table in tables:
                node.add_leaf(label=table, data=self.Table(table))
        else:
            node.add_leaf("No tables found")

    def get_selected_db_id(self) -> str | None:
        tree: Tree[DBConnection] = self.query_one(Tree)  # pyright: ignore [reportUnknownVariableType]
        cursor_node = tree.cursor_node

        if cursor_node is None or cursor_node == tree.root:
            return None

        if cursor_node.data:
            return cursor_node.data.id

        return None

    def _fetch_tables(self, connection: DBConnection) -> None | list[str] | list[Any]:
        tables = []
        try:
            if self.postgres_service.connect(connection):
                tables = self.postgres_service.list_tables()
            else:
                self.app.notify(
                    f"Failed to connect to database {connection.name}",
                    title="Connection Error",
                    severity="error",
                )
        except Exception as e:
            self.app.notify(
                f"Error listing tables: {str(e)}",
                title="Error",
                severity="error",
            )
        finally:
            self.postgres_service.disconnect()

        return tables
