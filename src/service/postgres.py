from typing import Any

import psycopg2


class PostgresService:
    connection: psycopg2.extensions.connection | None = None
    cursor: psycopg2.extensions.cursor | None = None

    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="password",
                host="localhost",
                port="5432",
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

    def disconnect(self) -> bool:
        if not self.connection:
            return True

        try:
            if self.cursor:
                self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None
            return True
        except Exception as e:
            print(f"Error disconnecting from the database: {e}")
            return False


    def list_tables(self) -> list[str]:
        self._connection_sanity_check()
        assert self.cursor is not None
        assert self.connection is not None

        try:
            self.cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            return [table[0] for table in self.cursor.fetchall()]
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def execute_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]] | int:
        self._connection_sanity_check()
        assert self.cursor is not None
        assert self.connection is not None

        try:
            self.cursor.execute(query, params or {})

            if query.strip().upper().startswith("SELECT"):
                result = self.cursor.fetchall()
                # Convert from psycopg2's DictRow to plain dictionaries
                return [dict(row) for row in result]
            else:
                # For non-SELECT queries, return affected row count
                self.connection.commit()
                return self.cursor.rowcount
        except Exception as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
            raise

    def _connection_sanity_check(self) -> None:
        if self.connection is None or self.cursor is None or not self.is_connected():
            raise ConnectionError("Not connected to the database")

    def is_connected(self) -> bool:
        if self.connection is None or self.cursor is None:
            return False

        try:
            self.cursor.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"Error checking connection to the database: {e}")
            return False
