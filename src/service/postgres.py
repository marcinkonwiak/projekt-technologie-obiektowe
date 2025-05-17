from typing import Any, Literal

import psycopg2
from psycopg2 import sql

from src.settings import DBConnection


class PostgresService:
    connection: psycopg2.extensions.connection | None = None
    cursor: psycopg2.extensions.cursor | None = None

    def connect(self, db_config: DBConnection) -> bool:
        try:
            self.connection = psycopg2.connect(
                dbname=db_config.database,
                user=db_config.user,
                password=db_config.password,
                host=db_config.host,
                port=db_config.port,
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

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

    def get_data(
        self,
        table: str,
        columns: list[str],
        *,
        limit: int = 500,
        order_by_column: str | None = None,
        order_by_direction: Literal["ASC", "DESC"] = "ASC",
    ) -> tuple[list[tuple[Any, ...]], str]:
        self._connection_sanity_check()
        assert self.cursor is not None
        assert self.connection is not None

        column_idents = [sql.Identifier(col) for col in columns]

        if order_by_column is not None:
            order_by_clause = sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(order_by_column),
                sql.SQL(order_by_direction),
            )
        else:
            order_by_clause = sql.SQL("")

        query = sql.SQL("SELECT {} FROM {}{} LIMIT {}").format(
            sql.SQL(", ").join(column_idents),
            sql.Identifier(table),
            order_by_clause,
            sql.Literal(limit),
        )
        query_string = query.as_string(self.connection)
        self.cursor.execute(query)

        return self.cursor.fetchall(), query_string

    def get_table_columns(self, table: str) -> list[str]:
        self._connection_sanity_check()
        assert self.cursor is not None
        assert self.connection is not None

        self.cursor.execute(
            sql.SQL(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s"
            ),
            [table],
        )
        return [column[0] for column in self.cursor.fetchall()]
