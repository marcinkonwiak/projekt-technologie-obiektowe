import json
import os
import uuid
from pathlib import Path

from pydantic_settings import BaseSettings


class DBConnection(BaseSettings):
    id: str = ""
    name: str
    host: str | None
    port: int | None
    user: str | None
    password: str | None
    database: str | None


class AppConfig(BaseSettings):
    config_path: Path = Path.home() / ".config" / "db_explorer" / "config.json"

    db_connections: list[DBConnection] = []

    @classmethod
    def load(cls, config_path: Path | None = None) -> "AppConfig":
        """Load configuration from a JSON file if it exists, otherwise create default"""
        path = config_path or cls().config_path

        # Ensure directory exists
        path.parent.mkdir(exist_ok=True, parents=True)

        # Try to load existing config
        if path.exists():
            try:
                with open(path) as f:
                    config_data = json.load(f)
                return cls(**config_data, config_path=path)
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error loading config: {e}. Using defaults.")

        # Create a new config with defaults
        config = cls(config_path=path)
        config.save()
        return config

    def save(self) -> None:
        """Save configuration to file"""
        self.config_path.parent.mkdir(exist_ok=True, parents=True)

        config_dict = self.model_dump(exclude={"config_path"})

        with open(self.config_path, "w") as f:
            json.dump(config_dict, f, indent=2, default=str)

        os.chmod(self.config_path, 0o600)  # Read/write for owner only

    def add_db_connection(self, connection: DBConnection) -> None:
        if not connection.id:
            connection.id = str(uuid.uuid4())
        self.db_connections.append(connection)
        self.save()

    def remove_db_connection(self, connection_id: str) -> bool:
        for i, connection in enumerate(self.db_connections):
            if connection.id == connection_id:
                del self.db_connections[i]
                self.save()
                return True
        return False

    def get_db_connection(self, index: int) -> DBConnection | None:
        return (
            self.db_connections[index]
            if 0 <= index < len(self.db_connections)
            else None
        )
