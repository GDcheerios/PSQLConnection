import psycopg2
import time


class PSQLConnection:
    """
    A utility class for managing PostgreSQL database connections
    and performing queries.
    """
    _db_connection = None
    _db_cursor = None
    _DEFAULT_PORT = 5432

    @staticmethod
    def connect(
            user: str,
            password: str,
            host: str,
            database: str,
            port: int = _DEFAULT_PORT,
    ) -> None:
        """
        Establishes a connection to the PostgreSQL database.
        """
        PSQLConnection._db_connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        if PSQLConnection._db_connection:
            print(f"Connected to {database} DB successfully.")
        PSQLConnection._db_cursor = PSQLConnection._db_connection.cursor()

    @staticmethod
    def _log_execution_time(action_description: str, start_time: float) -> None:
        """
        Logs the execution time for a database operation.
        """
        duration = time.time() - start_time
        print(f"{action_description} in {duration:.2f} seconds!")

    @staticmethod
    def _run_query(query: str, params: tuple = (), fetch_mode: str = None):
        """
        Executes the given query and optionally fetches results.

        :param query: The SQL query to be executed.
        :param params: Parameters to be passed into the SQL query.
        :param fetch_mode: Determines the fetch behavior:
                           - None: Execute query without returning results.
                           - "all": Fetch all rows.
                           - "one": Fetch a single row.
        :return: Fetched rows (if fetch_mode is specified), otherwise None.
        """
        start = time.time()
        try:
            PSQLConnection._db_cursor.execute(query, params)
            PSQLConnection._db_connection.commit()

            # Commit for data modification queries
            if fetch_mode is None:
                PSQLConnection._log_execution_time("Query executed", start)
                return None
            elif fetch_mode == "all":
                results = PSQLConnection._db_cursor.fetchall()
                PSQLConnection._log_execution_time(f"{len(results)} results fetched", start)
                return results
            elif fetch_mode == "one":
                result = PSQLConnection._db_cursor.fetchone()
                PSQLConnection._log_execution_time("Result fetched", start)
                return result
            elif fetch_mode == "all_as_dict":
                results = PSQLConnection._db_cursor.fetchall()
                PSQLConnection._log_execution_time(f"{len(results)} results fetched", start)
                column_names = [desc[0] for desc in PSQLConnection._db_cursor.description]
                if not results: return None
                if not column_names: return None
                return [dict(zip(column_names, row)) for row in results]
            elif fetch_mode == "one_as_dict":
                result = PSQLConnection._db_cursor.fetchone()
                PSQLConnection._log_execution_time("Result fetched", start)
                column_names = [desc[0] for desc in PSQLConnection._db_cursor.description]
                if not result: return None
                if not column_names: return None
                return dict(zip(column_names, result))
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")

    @staticmethod
    def execute(query: str, params: tuple = ()) -> None:
        """
        Executes a query that modifies the database.
        """
        PSQLConnection._run_query(query, params, fetch_mode=None)

    @staticmethod
    def fetch_all(query: str, params: tuple = ()) -> list:
        """
        Fetches all results from a query.
        """
        return PSQLConnection._run_query(query, params, fetch_mode="all")

    @staticmethod
    def fetch_one(query: str, params: tuple = ()) -> tuple:
        """
        Fetches a single result from a query.
        """
        return PSQLConnection._run_query(query, params, fetch_mode="one")

    @staticmethod
    def fetch_all_to_dict(query: str, params: tuple = ()) -> list:
        """
         Fetches all results from a query and converts them to a dictionary.
        """
        return PSQLConnection._run_query(query, params, fetch_mode="all_as_dict")

    @staticmethod
    def fetch_to_dict(query: str, params: tuple = ()) -> dict:
        """
         Fetches all results from a query and converts them to a dictionary.
        """
        return PSQLConnection._run_query(query, params, fetch_mode="one_as_dict")

    @staticmethod
    def now():
        return PSQLConnection._run_query("SELECT NOW()")

    @staticmethod
    def end() -> None:
        """
        Properly closes the database connection and cursor.
        """
        if PSQLConnection._db_cursor:
            PSQLConnection._db_cursor.close()
        if PSQLConnection._db_connection:
            PSQLConnection._db_connection.close()
        print("Database connection closed.")
