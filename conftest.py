from pathlib import Path

import pytest

from src.settings import AppConfig


@pytest.fixture
def temp_config_path(tmp_path: Path) -> Path:
    return tmp_path / "config.json"


@pytest.fixture
def sample_db_config() -> dict[str, str | int]:
    return {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }


@pytest.fixture
def config_with_connection(
    temp_config_path: Path, sample_db_config: dict[str, str | int]
) -> AppConfig:
    config = AppConfig(config_path=temp_config_path)
    config.add_db_connection("test_conn", sample_db_config)
    return config
