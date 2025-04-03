from src.app import DatabaseApp
from src.settings import AppConfig

if __name__ == "__main__":
    config = AppConfig.load()

    app = DatabaseApp(config=config)
    app.run()
