# %%
import pyodbc
import pandas as pd
#%%
class DatabricksOdbcConnector:
    
    def __init__(self):
        self.dsn_name:str = "Databricks"
        self.connection = None

    def connect(self):
        # Establish a connection
        connection_string:str = f"DSN={self.dsn_name}"
        self.connection = pyodbc.connect(connection_string, autocommit=True)

    def execute_query(self, sql_query:str):
        if self.connection is None:
            raise Exception("Not connected to Databricks. Call connect() first.")
        
        # Execute the query and fetch the result into a Pandas DataFrame
        result_df = pd.read_sql(sql_query, self.connection)
        return result_df

    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None