from typing import TYPE_CHECKING, Any, Literal

import psycopg2
from psycopg2 import sql
from textual import log

from src.service.types import Column, TableMetadata
from src.settings import DBConnection
from src.types import QueryOption, QueryOptionCondition

if TYPE_CHECKING:
    pass


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

    def get_table_columns_metadata(self, table: str) -> TableMetadata:
        self._connection_sanity_check()
        assert self.cursor is not None
        assert self.connection is not None

        self.cursor.execute(
            sql.SQL("""
            WITH foreign_key_info AS (SELECT con.conname             AS constraint_name,
                                             con.conrelid::regclass  AS referencing_table,
                                             a_ref.attname           AS referencing_column,
                                             con.confrelid::regclass AS referenced_table,
                                             a_fkey.attname          AS referenced_column,
                                             ns_ref.nspname          AS referencing_schema_name,
                                             c_ref.relname           AS referencing_table_name_only,
                                             ns_fkey.nspname         AS referenced_schema_name,
                                             c_fkey.relname          AS referenced_table_name_only
                                      FROM pg_constraint con
                                               JOIN
                                           pg_class c_ref ON con.conrelid = c_ref.oid
                                               JOIN
                                           pg_namespace ns_ref ON c_ref.relnamespace = ns_ref.oid
                                               JOIN
                                           pg_class c_fkey ON con.confrelid = c_fkey.oid
                                               JOIN
                                           pg_namespace ns_fkey ON c_fkey.relnamespace = ns_fkey.oid
                                               JOIN
                                           UNNEST(con.conkey) WITH ORDINALITY AS u_ref(attnum, ord)
                                           ON TRUE
                                               JOIN
                                           pg_attribute a_ref ON u_ref.attnum = a_ref.attnum AND
                                                                 a_ref.attrelid = con.conrelid
                                               JOIN
                                           UNNEST(con.confkey) WITH ORDINALITY AS u_fkey(attnum, ord)
                                           ON u_ref.ord = u_fkey.ord
                                               JOIN
                                           pg_attribute a_fkey ON u_fkey.attnum = a_fkey.attnum AND
                                                                  a_fkey.attrelid = con.confrelid
                                      WHERE con.contype = 'f')
            SELECT isc.column_name,
                   isc.data_type         AS column_type,
                   CASE
                       WHEN fki.referencing_column IS NOT NULL THEN TRUE
                       ELSE FALSE
                       END               AS is_foreign_key,
                   fki.referenced_table  AS fk_references_table,
                   fki.referenced_column AS fk_references_column,
                   fki.constraint_name   AS fk_constraint_name
            FROM information_schema.columns isc
                     LEFT JOIN
                 foreign_key_info fki ON isc.table_schema = fki.referencing_schema_name
                     AND isc.table_name = fki.referencing_table_name_only
                     AND isc.column_name = fki.referencing_column
            WHERE isc.table_schema = 'public'
              AND isc.table_name = %s
            ORDER BY isc.ordinal_position;
            """),
            [table],
        )
        columns = self.cursor.fetchall()

        return TableMetadata(
            table_name=table,
            columns=[
                Column(
                    name=column[0],
                    type=column[1],
                    is_foreign_key=column[2],
                    foreign_key_table=column[3],
                    foreign_key_column=column[4],
                )
                for column in columns
            ],
        )

    def build_query_with_options(
        self,
        table_name: str,
        base_columns: list[Column],
        query_options: list[QueryOption],
    ) -> sql.Composed:
        """
        Builds a SQL query based on a list of QueryOption objects.

        Args:
            table_name: The name of the table to query.
            base_columns: A list of column names to select if no aggregate options are provided.
            query_options: A list of QueryOption objects to define aggregates and WHERE conditions.

        Returns:
            A psycopg2.sql.Composed object representing the built query.

        Raises:
            ConnectionError: If not connected to the database.
            ValueError: If inputs are invalid (e.g., no base_columns and no aggregates,
                        or aggregates provided but result in no selectable fields).
        """
        log(query_options)
        log(base_columns)
        log(table_name)

        self._connection_sanity_check()

        aggregate_opts: list[QueryOption] = []
        where_opts: list[QueryOption] = []
        join_opts: list[QueryOption] = []

        # Separate query options
        for opt in query_options:
            if opt.condition in {
                QueryOptionCondition.SUM,
                QueryOptionCondition.COUNT,
                QueryOptionCondition.AVG,
                QueryOptionCondition.MAX,
                QueryOptionCondition.MIN,
            }:
                aggregate_opts.append(opt)
            elif opt.condition == QueryOptionCondition.WHERE:
                where_opts.append(opt)
            elif opt.condition in {
                QueryOptionCondition.LEFT_JOIN,
                QueryOptionCondition.INNER_JOIN,
            }:
                join_opts.append(opt)

        # SELECT clause
        select_expressions: list[sql.Composed] = []
        if aggregate_opts:
            for agg_opt in aggregate_opts:
                if agg_opt.column_name:  # Ensure column_name is present
                    select_expressions.append(
                        sql.SQL("{}({})").format(
                            sql.SQL(
                                agg_opt.condition.value.upper()
                            ),  # e.g., SUM, COUNT
                            sql.Identifier(agg_opt.column_name),
                        )
                    )
            if not select_expressions:
                raise ValueError(
                    "Aggregate options were provided but resulted in no selectable fields. "
                    "Ensure column names are specified for all aggregate options."
                )
            select_clause = sql.SQL(", ").join(select_expressions)
        else:
            if not base_columns:
                raise ValueError(
                    "base_columns must be provided if no aggregate options are specified."
                )
            select_clause = sql.SQL(", ").join(
                map(lambda col: sql.Identifier(col.name), base_columns)
            )

        # FROM clause
        from_clause = sql.SQL("").join(
            [sql.Identifier(table_name)]
            + [
                sql.SQL(" {} JOIN {} ON {}.{} = {}.{}").format(
                    sql.SQL(
                        opt.condition.value.upper().replace("_", " ")
                    ),  # LEFT JOIN or INNER JOIN
                    sql.Identifier(opt.join_to_table),
                    sql.Identifier(
                        table_name
                    ),  # Assuming join is always from the base table
                    sql.Identifier(
                        opt.column_name
                    ),  # This is the column in the base table
                    sql.Identifier(opt.join_to_table),
                    sql.Identifier(
                        opt.join_to_column
                    ),  # This is the column in the table being joined
                )
                for opt in join_opts
                if opt.join_to_table and opt.column_name and opt.join_to_column
            ]
        )

        # WHERE clause
        where_conditions: list[sql.Composed] = []
        if where_opts:
            for wh_opt in where_opts:
                if wh_opt.column_name and wh_opt.where_condition:
                    condition_upper = wh_opt.where_condition.upper()
                    if condition_upper in ("IS NULL", "IS NOT NULL"):
                        where_conditions.append(
                            sql.SQL("{} {}").format(
                                sql.Identifier(wh_opt.column_name),
                                sql.SQL(
                                    wh_opt.where_condition.upper()
                                ),  # Use upper for consistency
                            )
                        )
                    elif wh_opt.where_value is not None:
                        where_conditions.append(
                            sql.SQL("{} {} {}").format(
                                sql.Identifier(wh_opt.column_name),
                                sql.SQL(
                                    wh_opt.where_condition
                                ),  # Operator, e.g., '=', '>', 'LIKE'
                                sql.Literal(wh_opt.where_value),
                            )
                        )
                    # Silently skip malformed where options (e.g., condition requiring value but value is None)

        where_sql_clause = sql.SQL("")
        if where_conditions:
            where_sql_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(
                where_conditions
            )

        # Assemble the query
        final_query = sql.SQL("SELECT {} FROM {}{}").format(
            select_clause,
            from_clause,
            where_sql_clause,
        )

        return final_query
