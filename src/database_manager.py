# %%
import pyodbc

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
        
# %%
# db_manager = DatabaseManager()
# print(db_manager.sql_is_connected())
# %%
