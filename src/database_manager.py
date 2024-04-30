# %%
import pyodbc
import pandas as pd
from custom_logger import CustomLogger

#%%
# Create an instance of CustomLogger with logger name and log directory
logger_instance = CustomLogger("DataBase_Utils","src\logs")

# Get the logger
logger = logger_instance.get_logger()
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

            row_values = row.apply(lambda x: x if pd.notnull(x) else None)
            placeholders = ', '.join(['?'] * len(row_values))
            query = f"INSERT INTO {sql_table_name} {column_tuple_str} VALUES ({placeholders});"
            try:    
                self.cursor.execute(query, tuple(row_values))
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error adding row to {sql_table_name}: {tuple(row_values)}.")

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
