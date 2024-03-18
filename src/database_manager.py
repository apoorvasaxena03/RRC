# %%
import pyodbc
import pandas as pd

class DatabaseManager:
    """ This is a class to manage MsSQL Database.
    """

    def __init__(self):
        """Constructor to initialize the attributes of the class.

        Args:
            num1 (int): accepts an integer value.
            num2 (int): accepts an integer value.
        """
        self.driver = '{SQL Server}'
        self.server = 'TXDC-LHACNDB01'
        self.trusted_connection = 'yes'

    def sql_is_connected(self,dbname='master'):
        """Function to check if the datbase connection is succesfuly made to given database.

        Args:
            dbname (str): defaults database is master.

        Returns:
            Bool
        """

        try:
            conn = pyodbc.connect(
            driver = self.driver,
            server = self.server,
            database = dbname,
            trusted_connection = self.trusted_connection
            )
            conn.close()
            return True
        
        except pyodbc.Error as e:
            print(f"Connection not available to database {dbname}", e)
            return False
        
    def connect_to_db(self, dbname):
        """Function to connect to Database and return connection.

        Args:
            dbname (str): Database name that you want to connect to in MsSQL.

        Returns:
            Connection to MsSQL.
        """
        try:
            conn = pyodbc.connect(
            driver = self.driver,
            server = self.server,
            database = dbname,
            trusted_connection = self.trusted_connection
            )
        except pyodbc.Error as e:
            print(f"Error returning connection from {dbname}: {e}")
        else:
            print(f'Connected to {dbname}!')
            return conn
#%%
class MSSqlConnector:
    
    def __init__(self)-> None:
        self.driver:str = '{SQL Server}'
        self.server:str = 'TXDC-LHACNDB01'
        self.trusted_connection:str = 'yes'
        self.connection = None

    def connect(self, dbname:str = 'master'):
        # Establish a connection
        self.connection = pyodbc.connect(
            driver = self.driver,
            server = self.server,
            database = dbname,
            trusted_connection = self.trusted_connection
        )

    def execute_query(self, sql_query:str):
        if self.connection is None:
            raise Exception("Not connected to SQL Server. Call connect() first.")
        
        # Execute the query and fetch the result into a Pandas DataFrame
        result_df = pd.read_sql(sql_query, self.connection)
        return result_df

    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

class SQLTableManager:
    def __init__(self) -> None:
        self.driver:str = '{SQL Server}'
        self.server:str = 'TXDC-LHACNDB01'
        self.trusted_connection:str = 'yes'
        self.connection = None

    def connect(self, dbname:str = 'master'):
        # Establish a connection
        self.connection = pyodbc.connect(
            driver = self.driver,
            server = self.server,
            database = dbname,
            trusted_connection = self.trusted_connection
        )
        self.cursor = self.connection.cursor()

    def add_rows_from_dataframe(self, sql_table_name, dataframe):
        
        # Convert column names to tuple without apostrophes
        column_tuple = tuple(dataframe.columns)
        # Remove apostrophes from the string representation of the tuple
        column_tuple_str = str(column_tuple).replace("'", "")

        for index, row in dataframe.iterrows():
            placeholders = ', '.join(['?'] * len(row))
            query = f"INSERT INTO {sql_table_name} {column_tuple_str} VALUES ({placeholders});"
            self.cursor.execute(query, tuple(row))
        self.connection.commit()

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()
            self.connection = None
# %%
# db_manager = DatabaseManager()
# print(db_manager.sql_is_connected())
# %%
