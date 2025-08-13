import psycopg2
from psycopg2 import Error
from pandas import DataFrame
import pandas as pd


class DbQueryTool:
    """
    A utility class for interacting with databases such as  PostgreSQL.

    Attributes:
        database (str): The name of the database to connect to.
        host (str): The hostname or IP address of the database server.
        port (int): The port number to connect to the database server.
        user (str): The username for authenticating with the database.
        password (str): The password for authenticating with the database.
        connection: The database connection object, initialized based on the database type.

    Methods:
        __init__(dbname: str, host: str, port: int, user: str, password: str, db_type: str):
            Initializes the DbQueryTool instance and establishes a connection to the database.

        execute_sql(sql: str, get_output: bool = True):
            Executes the given SQL query on the connected database.
            If `get_output` is True, returns the query result as a pandas DataFrame.
            Otherwise, executes the query and commits the transaction without returning any output.
    """

    def __init__(self, dbname: str, host: str, port: int, user: str, password: str, db_type: str):
        """
        Initializes a database connection object.

        Args:
            dbname (str): The name of the database to connect to.
            host (str): The hostname or IP address of the database server.
            port (int): The port number on which the database server is listening.
            user (str): The username to authenticate with the database.
            password (str): The password to authenticate with the database.
            db_type (str): The type of the database (e.g., 'Redshift', 'Postgress').

        Raises:
            Exception: If the `db_type` is not supported or if the connection fails.
        """
        self.database = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        if db_type == 'Postgress':
            self.connection = psycopg2.connect(
                dbname=self.database,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def execute_sql(self, sql: str, get_output: bool = True):
        """
        Executes an SQL query on the connected database.

        Args:
            sql (str): The SQL query to be executed.
            get_output (bool, optional): If True, the method fetches the query result 
                as a pandas DataFrame. If False, the method executes the query without 
                fetching results. Defaults to True.

        Returns:
            pandas.DataFrame or None: Returns a DataFrame containing the query results 
                if `get_output` is True. Returns None if `get_output` is False.
        """
        if get_output:
            df = pd.read_sql_query(sql, self.connection)
            return df
        else:
            cur = self.connection.cursor()
            cur.execute(sql)
            self.connection.commit()
