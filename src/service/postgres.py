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
        if self.connection is None or self.cursor is None or self.connection.closed:
            if self.connection and self.connection.closed:
                log.warning("Connection previously marked as closed.")
                # Ensure state is clean if connection.closed was the trigger
                self.connection = None
                self.cursor = None
            return False

        try:
            # Use a temporary cursor for the health check to avoid issues with the shared cursor state
            with self.connection.cursor() as temp_cursor:
                temp_cursor.execute("SELECT 1")
            return True
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log.warning(f"Connection test query failed, connection seems dead: {e}")
            # Attempt to clean up the dead connection
            try:
                if not self.connection.closed:  # Check if not already closed
                    self.connection.close()
            except Exception as close_exc:
                log.error(f"Error while closing dead connection: {close_exc}")
            finally:
                self.connection = None
                self.cursor = None
            return False
        except Exception as e:
            log.error(f"Unexpected error during connection check: {e}")
            # For other unexpected errors, also assume connection is compromised
            self.connection = None
            self.cursor = None
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

    def get_data_from_query(
        self,
        query: sql.Composed,
    ) -> tuple[list[tuple[Any, ...]], str, list[str]]:
        """
        Executes a composed query and fetches all results.

        Args:
            query: A psycopg2.sql.Composed query object.

        Returns:
            A tuple containing the data, the executed query string, and the list of column names.

        Raises:
            ConnectionError: If not connected to the database.
            psycopg2.Error: If any database error occurs during query execution.
        """
        self._connection_sanity_check()
        assert self.cursor is not None  # Ensured by _connection_sanity_check
        assert self.connection is not None  # Ensured by _connection_sanity_check

        query_string = query.as_string(self.connection)
        log(f"Executing query: {query_string}")

        try:
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            # Get column names from cursor.description
            column_names: list[str] = []
            if self.cursor.description:
                column_names = [str(desc[0]) for desc in self.cursor.description]  # type: ignore[misc]
            return data, query_string, column_names
        except psycopg2.Error as e:  # Catch psycopg2 specific errors
            log.error(f"Database error in get_data_from_query: {e}")
            if self.connection:
                try:
                    self.connection.rollback()  # Attempt to rollback
                    log.info("Transaction rolled back due to query error.")
                except psycopg2.Error as rb_exc:
                    # Log if rollback itself fails, but prioritize original error
                    log.error(f"Error during rollback attempt: {rb_exc}")
            raise  # Re-raise the original exception to be handled by the caller
        except Exception as e:
            # Catch other potential errors, though psycopg2.Error should cover most DB issues.
            # It's less likely we'd need a rollback here, but consider if specific non-DB errors
            # could also leave the transaction in a bad state.
            log.error(f"Unexpected error in get_data_from_query: {e}")
            # Not explicitly rolling back here, as it's not a psycopg2.Error
            raise

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
        order_by_column: str | None = None,
        order_by_direction: Literal["ASC", "DESC"] = "ASC",
    ) -> sql.Composed:
        """
        Builds a SQL query based on a list of QueryOption objects.

        Args:
            table_name: The name of the table to query.
            base_columns: A list of column names to select if no aggregate options are provided.
            query_options: A list of QueryOption objects to define aggregates and WHERE conditions.
            order_by_column: The column to order the results by.
            order_by_direction: The direction to order the results by.

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
                if agg_opt.column_name:
                    if agg_opt.column_name == "*":
                        select_expressions.append(
                            sql.SQL("{}(*)").format(
                                sql.SQL(agg_opt.condition.value.upper())
                            )
                        )
                    else:
                        alias = sql.Identifier(
                            f"{agg_opt.condition.value}_{agg_opt.column_name}"
                        )
                        select_expressions.append(
                            sql.SQL("{}({}.{}) AS {}").format(
                                sql.SQL(agg_opt.condition.value.upper()),
                                sql.Identifier(table_name),
                                sql.Identifier(agg_opt.column_name),
                                alias,
                            )
                        )
            if not select_expressions:
                raise ValueError("Aggregate options resulted in no selectable fields.")
            select_clause = sql.SQL(", ").join(select_expressions)
        else:
            # Base table columns
            for col in base_columns:
                alias = sql.Identifier(f"{table_name}.{col.name}")
                select_expressions.append(
                    sql.SQL("{}.{} AS {}").format(
                        sql.Identifier(table_name), sql.Identifier(col.name), alias
                    )
                )

            # Joined tables columns
            processed_joined_tables: set[str] = set()
            for join_opt in join_opts:
                if (
                    join_opt.join_to_table
                    and join_opt.join_to_table not in processed_joined_tables
                ):
                    try:
                        joined_table_meta = self.get_table_columns_metadata(
                            join_opt.join_to_table
                        )
                        for col in joined_table_meta.columns:
                            alias = sql.Identifier(
                                f"{join_opt.join_to_table}.{col.name}"
                            )
                            select_expressions.append(
                                sql.SQL("{}.{} AS {}").format(
                                    sql.Identifier(join_opt.join_to_table),
                                    sql.Identifier(col.name),
                                    alias,
                                )
                            )
                        processed_joined_tables.add(join_opt.join_to_table)
                    except Exception as e:
                        log.error(
                            f"Could not get metadata for joined table {join_opt.join_to_table}: {e}"
                        )
                        pass

            if not select_expressions:
                raise ValueError(
                    "No columns to select. Base table might have no columns or joined tables failed."
                )
            select_clause = sql.SQL(", ").join(select_expressions)

        # FROM clause
        from_clause = sql.SQL("").join(
            [sql.Identifier(table_name)]
            + [
                sql.SQL(" {} {} ON {}.{} = {}.{}").format(
                    sql.SQL(opt.condition.value.upper().replace("_", " ")),
                    sql.Identifier(opt.join_to_table),
                    sql.Identifier(table_name),
                    sql.Identifier(opt.column_name),
                    sql.Identifier(opt.join_to_table),
                    sql.Identifier(opt.join_to_column),
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

        # ORDER BY clause
        order_by_sql_clause = sql.SQL("")
        if order_by_column:
            # Directly use the order_by_column.
            # It's assumed this column name is valid in the context of the final SELECT list.
            # (e.g. it's an alias, or a unique name like table.column if that's how it appears in results)
            order_by_sql_clause = sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(
                    order_by_column
                ),  # Use Identifier for safety if it's a simple column name
                sql.SQL(order_by_direction),
            )

        # Assemble the query
        final_query = sql.SQL("SELECT {} FROM {}{}{}").format(
            select_clause,
            from_clause,
            where_sql_clause,
            order_by_sql_clause,  # Add order by clause
        )

        return final_query
