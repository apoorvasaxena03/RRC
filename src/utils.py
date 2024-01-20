import pyodbc


# %%
def connect_to_db(dbname):
    """      
    Parameters
    ----------
    dbname : TYPE, STR
        DESCRIPTION. Database name that you want to connect to in MsSQL

    Returns
    -------
    Connection to MsSQL.
    """
    try:
        conn = pyodbc.connect(
                driver='{SQL Server}',
                server='TXDC-LHACNDB01',
                database = dbname,
                trusted_connection='yes'
                )

    except Exception as err:
        print(f'Something went wrong: {err}')
    else:
        print(f'Connected to {dbname}!')
        return conn
# %%
