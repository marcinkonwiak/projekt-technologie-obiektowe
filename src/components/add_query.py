from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.reactive import Reactive
from textual.screen import Screen
from textual.widgets import Button, Label, Select

from src.service.types import TableMetadata
from src.types import QueryOptionCondition


@dataclass
class AddQueryOptionModalScreenResult:
    condition: QueryOptionCondition
    column_name: str


class AddQueryOptionModalScreen(Screen[AddQueryOptionModalScreenResult]):
    CSS_PATH = "../css/add_query_option_modal.tcss"

    _conditions = [
        ("Where", QueryOptionCondition.WHERE),
        ("Left Join", QueryOptionCondition.LEFT_JOIN),
        ("Inner Join", QueryOptionCondition.INNER_JOIN),
        ("Sum", QueryOptionCondition.SUM),
        ("Count", QueryOptionCondition.COUNT),
        ("Avg", QueryOptionCondition.AVG),
        ("Max", QueryOptionCondition.MAX),
        ("Min", QueryOptionCondition.MIN),
    ]

    _selected_condition: Reactive[QueryOptionCondition | None] = Reactive(None)
    _table_metadata: TableMetadata

    def __init__(
        self,
        *,
        table_metadata: TableMetadata,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        self._table_metadata = table_metadata

        super().__init__(name, id, classes)

    def compose(self) -> ComposeResult:
        with Vertical(id="modal"):
            yield Label("Add a new Query option", id="add-query-option-label")

            yield Label("condition", classes="field-label")
            yield Select(self._conditions, id="condition-field")

            yield Container(id="condition-content")

            with Container(id="button-grid"):
                yield Button("Add", variant="primary", id="add-button")
                yield Button("Cancel", variant="default", id="cancel-button")

    def watch__selected_condition(
        self,
        old_value: QueryOptionCondition | None,
        new_value: QueryOptionCondition | None,
    ) -> None:
        if not self.is_mounted:
            return

        container = self.query_one("#condition-content", Container)
        container.remove_children()

        if new_value == QueryOptionCondition.WHERE:
            container.mount(Label("WHERE", classes="field-label"))
        elif new_value == QueryOptionCondition.LEFT_JOIN:
            self._mount_join_content(container)
        elif new_value == QueryOptionCondition.INNER_JOIN:
            self._mount_join_content(container)
        elif new_value == QueryOptionCondition.SUM:
            self._mount_aggregate_content(container)
        elif new_value == QueryOptionCondition.COUNT:
            self._mount_aggregate_content(container)
        elif new_value == QueryOptionCondition.AVG:
            self._mount_aggregate_content(container)
        elif new_value == QueryOptionCondition.MAX:
            self._mount_aggregate_content(container)
        elif new_value == QueryOptionCondition.MIN:
            self._mount_aggregate_content(container)

    def _mount_join_content(self, container: Container) -> None:
        container.mount(
            Label("Join table", classes="field-label"),
            Select(
                [
                    (table_name, table_name)
                    for table_name in self._table_metadata.get_joinable_column_names()
                ],
                classes="column-name-select",
            ),
        )

    def _mount_aggregate_content(self, container: Container) -> None:
        container.mount(
            Label("Aggregate function", classes="field-label"),
            Select(
                [
                    (column_name, column_name)
                    for column_name in self._table_metadata.get_column_names()
                ],
                classes="column-name-select",
            ),
        )

    def on_select_changed(self, event: Select.Changed) -> None:
        assert isinstance(event.select, Select)
        if event.select.id == "condition-field":
            if isinstance(event.value, QueryOptionCondition):
                self._selected_condition = event.value
            else:
                self._selected_condition = None

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add-button":
            message = self.dispatch_add_message()
            if not message:
                return
            self.dismiss(message)
        elif event.button.id == "cancel-button":
            self.dismiss()

    def dispatch_add_message(self) -> AddQueryOptionModalScreenResult | None:
        condition = self._selected_condition

        try:
            select: Select[str] = self.query_one(".column-name-select", Select)  # pyright: ignore [reportUnknownVariableType]
            assert isinstance(select, Select)
            column_name = select.value
        except Exception:
            self.notify(
                "Invalid options selected",
                title="Error",
                severity="error",
                timeout=3,
            )
            return None

        if not condition or not column_name or not isinstance(column_name, str):
            self.notify(
                "Invalid options selected",
                title="Error",
                severity="error",
                timeout=3,
            )
            return None

        return AddQueryOptionModalScreenResult(
            condition=condition,
            column_name=column_name,
        )
