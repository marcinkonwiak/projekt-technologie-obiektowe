from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import Screen
from textual.widgets import Button, Input, Label


@dataclass
class AddConnectionModalScreenResult:
    name: str
    host: str | None
    port: int | None
    user: str | None
    password: str | None
    database: str | None


class AddConnectionModalScreen(Screen[AddConnectionModalScreenResult]):
    CSS_PATH = "../css/add_connection_modal.tcss"

    def compose(self) -> ComposeResult:
        with Vertical(id="modal"):
            yield Label("Add a new connection", id="add-db-connection-label")

            yield Label("name", classes="field-label")
            yield Input(id="name-input", placeholder="name")
            yield Label("host", classes="field-label")
            yield Input(id="host-input", placeholder="host")
            yield Label("port", classes="field-label")
            yield Input(
                id="port-input", placeholder="port", type="integer", valid_empty=True
            )
            yield Label("user", classes="field-label")
            yield Input(id="user-input", placeholder="user")
            yield Label("password", classes="field-label")
            yield Input(id="password-input", placeholder="password", password=True)
            yield Label("database", classes="field-label")
            yield Input(id="database-input", placeholder="database")

            with Container(id="button-grid"):
                yield Button("Add", variant="primary", id="add-button")
                yield Button("Cancel", variant="default", id="cancel-button")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add-button":
            self.dismiss(self.create_connection_create_message())
        elif event.button.id == "cancel-button":
            self.dismiss()

    def create_connection_create_message(self) -> AddConnectionModalScreenResult:
        name = self.query_one("#name-input")
        assert isinstance(name, Input)
        host = self.query_one("#host-input")
        assert isinstance(host, Input)
        port = self.query_one("#port-input")
        assert isinstance(port, Input)
        user = self.query_one("#user-input")
        assert isinstance(user, Input)
        password = self.query_one("#password-input")
        assert isinstance(password, Input)
        database = self.query_one("#database-input")
        assert isinstance(database, Input)

        return AddConnectionModalScreenResult(
            name=name.value,
            host=host.value,
            port=int(port.value) if port.value else None,
            user=user.value,
            password=password.value,
            database=database.value,
        )
