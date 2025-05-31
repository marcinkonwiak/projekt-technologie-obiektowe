from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.reactive import Reactive
from textual.screen import Screen
from textual.widgets import Button, Input, Label, Select
from textual.widgets._select import NoSelection

from src.service.types import TableMetadata
from src.types import QueryOptionCondition


@dataclass
class AddQueryOptionModalScreenResult:
    column_name: str
    condition: QueryOptionCondition
    where_condition: str | None = None
    where_value: str | None = None
    join_to_table: str | None = None
    join_on_column: str | None = None
    join_to_column: str | None = None


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
            self._mount_where_content(container)
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

    def _mount_where_content(self, container: Container) -> None:
        container.mount(
            Label("Column name", classes="field-label"),
            Select(
                [
                    (column_name, column_name)
                    for column_name in self._table_metadata.get_column_names()
                ],
                classes="column-name-select",
            ),
            Label("Condition", classes="field-label"),
            Select(
                [
                    ("=", "="),
                    ("!=", "!="),
                    ("<", "<"),
                    (">", ">"),
                    ("<=", "<="),
                    (">=", ">="),
                    ("LIKE", "LIKE"),
                    ("ILIKE", "ILIKE"),
                    ("IS NULL", "IS NULL"),
                    ("IS NOT NULL", "IS NOT NULL"),
                ],
                classes="where-condition-select",
                id="where-condition-select",
            ),
            Label("Value", classes="field-label", id="where-value-label"),
            Input(classes="where-value-input", id="where-value-input"),
        )

    def _mount_join_content(self, container: Container) -> None:
        # Get columns that are foreign keys from the current table
        fk_columns = [
            (col.name, col.name)
            for col in self._table_metadata.columns
            if col.is_foreign_key
        ]
        if not fk_columns:
            container.mount(
                Label(
                    "No foreign key columns available in the current table to join on."
                )
            )
            return

        container.mount(
            Label("Join ON column (from current table)", classes="field-label"),
            Select(fk_columns, classes="join-on-column-select"),
            # The join_to_table and join_to_column will be derived from the selected fk_column
            # We can display them for clarity if needed, but they are not directly selected by the user here.
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
        elif event.select.id == "where-condition-select":
            value_label = self.query_one("#where-value-label", Label)
            value_input = self.query_one("#where-value-input", Input)
            # Ensure we are dealing with a string value before comparison
            selected_where_condition = (
                str(event.value) if event.value is not NoSelection else None
            )
            if selected_where_condition in ("IS NULL", "IS NOT NULL"):
                value_label.display = False
                value_input.display = False
            else:
                value_label.display = True
                value_input.display = True

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
        column_name_for_select_where_agg: str | None = None
        join_on_column_name: str | None = None
        join_to_table: str | None = None
        join_to_column: str | None = None

        try:
            if condition in {
                QueryOptionCondition.LEFT_JOIN,
                QueryOptionCondition.INNER_JOIN,
            }:
                join_on_select_widget: Select[str] = self.query_one(
                    ".join-on-column-select", Select
                )
                raw_join_on_value = join_on_select_widget.value

                if raw_join_on_value is NoSelection:
                    raise ValueError("Join ON column must be selected.")
                join_on_column_name = str(raw_join_on_value)

                selected_fk_col_meta = next(
                    (
                        col
                        for col in self._table_metadata.columns
                        if col.name == join_on_column_name and col.is_foreign_key
                    ),
                    None,
                )
                if (
                    not selected_fk_col_meta
                    or not selected_fk_col_meta.foreign_key_table
                    or not selected_fk_col_meta.foreign_key_column
                ):
                    raise ValueError(
                        f"Could not find foreign key details for column {join_on_column_name}."
                    )
                join_to_table = selected_fk_col_meta.foreign_key_table
                join_to_column = selected_fk_col_meta.foreign_key_column
            else:
                column_select_widget: Select[str] | None = None
                try:
                    column_select_widget = self.query_one(".column-name-select", Select)
                except Exception:
                    if condition is not QueryOptionCondition.COUNT:
                        raise ValueError(
                            "Column selection UI element (.column-name-select) not found."
                        )

                temp_col_name_for_swa: str | None = None

                if column_select_widget:
                    raw_column_value = column_select_widget.value
                    if raw_column_value is NoSelection:
                        if condition is QueryOptionCondition.COUNT:
                            temp_col_name_for_swa = "*"
                        else:
                            # For WHERE and other aggregates, a column must be selected
                            raise ValueError(
                                "Column must be selected for this operation."
                            )
                    else:
                        temp_col_name_for_swa = str(raw_column_value)
                elif condition is QueryOptionCondition.COUNT:
                    temp_col_name_for_swa = "*"

                column_name_for_select_where_agg = temp_col_name_for_swa

        except Exception as e:
            self.notify(
                f"Invalid options selected: {str(e)}",
                title="Error",
                severity="error",
                timeout=3,
            )
            return None

        # Determine the primary column_name for QueryOption based on condition
        final_column_name = (
            join_on_column_name
            if condition
            in {QueryOptionCondition.LEFT_JOIN, QueryOptionCondition.INNER_JOIN}
            else column_name_for_select_where_agg
        )

        if not condition or not final_column_name:
            self.notify(
                "Invalid options selected",
                title="Error",
                severity="error",
                timeout=3,
            )
            return None

        where_condition = None
        where_value = None
        if condition == QueryOptionCondition.WHERE:
            try:
                where_condition_select: Select[str] = self.query_one(  # pyright: ignore [reportUnknownVariableType]
                    "#where-condition-select", Select
                )
                assert isinstance(where_condition_select, Select)
                where_condition = where_condition_select.value
                if not where_condition or not isinstance(where_condition, str):
                    raise ValueError("Where condition cannot be empty")

                # Only get where_value if the condition is not IS NULL or IS NOT NULL
                if where_condition not in ("IS NULL", "IS NOT NULL"):
                    where_value_input: Input = self.query_one(
                        "#where-value-input", Input
                    )
                    assert isinstance(where_value_input, Input)
                    where_value = where_value_input.value

                    if not where_value:
                        raise ValueError(
                            "Where value cannot be empty for this condition"
                        )
            except Exception as e:
                self.notify(
                    f"Invalid options selected: {str(e)}",
                    title="Error",
                    severity="error",
                    timeout=3,
                )
                return None

        return AddQueryOptionModalScreenResult(
            condition=condition,
            column_name=final_column_name,
            where_condition=where_condition,
            where_value=where_value,
            join_to_table=join_to_table,
            # join_on_column is now represented by column_name for JOINs
            join_to_column=join_to_column,
        )
