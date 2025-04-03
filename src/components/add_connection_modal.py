from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import Screen
from textual.widgets import Button, Input, Label


class AddConnectionModalScreen(Screen):
    CSS_PATH = "../css/add_connection_modal.tcss"

    def compose(self) -> ComposeResult:
        with Vertical(id="modal"):
            yield Label("Add a new connection", id="add-db-connection-label")

            yield Label("name", classes="field-label")
            yield Input(id="name-input", placeholder="name")

            yield Label("host", classes="field-label")
            yield Input(id="host-input", placeholder="host")

            yield Label("port", classes="field-label")
            yield Input(id="port-input", placeholder="port")

            yield Label("user", classes="field-label")
            yield Input(id="user-input", placeholder="user")

            yield Label("password", classes="field-label")
            yield Input(id="password-input", placeholder="password", password=True)

            yield Label("database", classes="field-label")
            yield Input(id="database-input", placeholder="database")

            with Container(id="button-grid"):
                yield Button("Test", variant="success", id="test-button")
                yield Button("Add", variant="primary", id="add-button")
                yield Button("Cancel", variant="default", id="cancel-button")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.app.exit()
        else:
            self.app.pop_screen()
