import json
import os
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings


class DBConnection(BaseSettings):
    host: str
    port: int
    user: str
    password: str
    database: str


class AppConfig(BaseSettings):
    config_path: Path = Path.home() / ".config" / "db_explorer" / "config.json"

    db_connections: dict[str, DBConnection] = {}

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
        # Ensure directory exists
        self.config_path.parent.mkdir(exist_ok=True, parents=True)

        # Convert to dict, excluding non-necessary fields
        config_dict = self.model_dump(exclude={"config_path"})

        with open(self.config_path, "w") as f:
            json.dump(config_dict, f, indent=2, default=str)

        os.chmod(self.config_path, 0o600)  # Read/write for owner only

    def add_db_connection(self, name: str, connection_details: dict[str, Any]) -> None:
        connection = DBConnection(**connection_details)
        self.db_connections[name] = connection
        self.save()

    def remove_db_connection(self, name: str) -> bool:
        if name in self.db_connections:
            del self.db_connections[name]
            self.save()
            return True
        return False

    def get_db_connection(self, name: str) -> DBConnection | None:
        return self.db_connections.get(name)
