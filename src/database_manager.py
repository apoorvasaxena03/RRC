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
        self.server='TXDC-LHACNDB01'
        self.trusted_connection='yes'

    def is_sql_connection_available(self):
        """Function to check if the datbase connection is succesfuly made to master database.

        Returns:
            Bool
        """

        try:
            conn = pyodbc.connect(
            driver=self.driver,
            server=self.server,
            database = 'master',
            trusted_connection='yes'
            )
            conn.close()
            return True
        
        except pyodbc.Error as e:
            print(f"Connection not available: {e}")
            return False
        
    def connect_to_db(self,dbname):
        """Function to connect to Database and return connection.

        Args:
            dbname (str): Database name that you want to connect to in MsSQL.

        Returns:
            Connection to MsSQL.
        """
        try:
            conn = pyodbc.connect(
            driver=self.driver,
            server=self.server,
            database = dbname,
            trusted_connection='yes'
            )
        except pyodbc.Error as e:
            print(f"Error returning connection from {dbname}: {e}")
        else:
            print(f'Connected to {dbname}!')
            return conn