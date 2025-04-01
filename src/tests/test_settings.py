import json
from pathlib import Path
from unittest.mock import patch

from src.settings import AppConfig, DBConnection


class TestDBConnection:
    def test_db_connection_creation(self, sample_db_config: dict[str, str | int]):
        connection = DBConnection(**sample_db_config)

        assert connection.host == sample_db_config["host"]
        assert connection.port == sample_db_config["port"]
        assert connection.user == sample_db_config["user"]
        assert connection.password == sample_db_config["password"]
        assert connection.database == sample_db_config["database"]


class TestAppConfig:
    def test_load_nonexistent_config(self, temp_config_path: Path):
        config = AppConfig.load(config_path=temp_config_path)

        assert config.config_path == temp_config_path
        assert config.db_connections == {}
        assert temp_config_path.exists()

    def test_load_valid_config(
        self, temp_config_path: Path, sample_db_config: dict[str, str | int]
    ):
        config_data = {"db_connections": {"test_conn": sample_db_config}}
        temp_config_path.parent.mkdir(exist_ok=True, parents=True)
        with open(temp_config_path, "w") as f:
            json.dump(config_data, f)

        config = AppConfig.load(config_path=temp_config_path)

        assert config.config_path == temp_config_path
        assert len(config.db_connections) == 1
        assert isinstance(config.db_connections["test_conn"], DBConnection)
        assert config.db_connections["test_conn"].host == sample_db_config["host"]

    def test_load_corrupted_config(self, temp_config_path: Path):
        # Create a corrupted config file
        temp_config_path.parent.mkdir(exist_ok=True, parents=True)
        with open(temp_config_path, "w") as f:
            f.write("This is not valid JSON")

        with patch("builtins.print") as mock_print:
            config = AppConfig.load(config_path=temp_config_path)

            mock_print.assert_called_once()
            assert "Error loading config" in mock_print.call_args[0][0]

        assert config.config_path == temp_config_path
        assert config.db_connections == {}

    def test_save_config(
        self, temp_config_path: Path, sample_db_config: dict[str, str | int]
    ):
        config = AppConfig(config_path=temp_config_path)
        config.add_db_connection("test_conn", sample_db_config)

        # Check file content
        with open(temp_config_path) as f:
            saved_data = json.load(f)

        assert "db_connections" in saved_data
        assert "test_conn" in saved_data["db_connections"]
        assert (
            saved_data["db_connections"]["test_conn"]["host"]
            == sample_db_config["host"]
        )

    def test_file_permissions(self, temp_config_path: Path):
        config = AppConfig(config_path=temp_config_path)
        config.save()

        assert oct(temp_config_path.stat().st_mode)[-3:] == "600"

    def test_add_db_connection(
        self, temp_config_path: Path, sample_db_config: dict[str, str | int]
    ):
        config = AppConfig(config_path=temp_config_path)
        config.add_db_connection("test_conn", sample_db_config)

        assert "test_conn" in config.db_connections
        assert isinstance(config.db_connections["test_conn"], DBConnection)
        assert config.db_connections["test_conn"].host == sample_db_config["host"]

    def test_remove_existing_db_connection(self, config_with_connection: AppConfig):
        result = config_with_connection.remove_db_connection("test_conn")

        assert result is True
        assert "test_conn" not in config_with_connection.db_connections

    def test_remove_nonexistent_db_connection(self, temp_config_path: Path):
        config = AppConfig(config_path=temp_config_path)
        result = config.remove_db_connection("nonexistent")

        assert result is False

    def test_get_existing_db_connection(
        self, config_with_connection: AppConfig, sample_db_config: dict[str, str | int]
    ):
        connection = config_with_connection.get_db_connection("test_conn")

        assert connection is not None
        assert isinstance(connection, DBConnection)
        assert connection.host == sample_db_config["host"]

    def test_get_nonexistent_db_connection(self, temp_config_path: Path):
        config = AppConfig(config_path=temp_config_path)
        connection = config.get_db_connection("nonexistent")

        assert connection is None

    def test_model_dump_excludes_config_path(self, config_with_connection: AppConfig):
        config_with_connection.save()

        with open(config_with_connection.config_path) as f:
            saved_data = json.load(f)

        assert "config_path" not in saved_data
