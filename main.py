from src.app import DatabaseApp
from src.settings import AppConfig

if __name__ == "__main__":
    settings = AppConfig.load()

    app = DatabaseApp()
    app.run()
