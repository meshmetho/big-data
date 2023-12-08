from sqlalchemy import create_engine
import pandas as pd
import pyodbc

# Set database credentials
db_password = 'metho123'
db_username = 'mesho'

# Database connection details
db_driver = "ODBC Driver 17 for SQL Server"
db_server = "localhost"
db_name = "AdventureWorksDW2019"

def extract_and_load():
    sql_query = """SELECT t.name AS table_name
                    FROM sys.tables t 
                    WHERE t.name IN ('DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales')"""

    try:
        connection_str = f"DRIVER={{{db_driver}}};SERVER={db_server};DATABASE={db_name};UID={db_username};PWD={db_password};"
        source_conn = pyodbc.connect(connection_str)
        print("Established connection...")

        source_cursor = source_conn.cursor()
        source_cursor.execute(sql_query)
        source_tables = source_cursor.fetchall()

        for table in source_tables:
            data_frame = pd.read_sql_query(f'SELECT * FROM {table[0]}', source_conn)
            load_to_postgres(data_frame, table[0])

    except Exception as error:
        print("Data extraction error: " + str(error))
    finally:
        source_conn.close()

def load_to_postgres(data_frame, table_name):
    try:
        rows_imported = 0
        postgres_engine = create_engine(f'postgresql://{"postgres"}:{"7934"}@{db_server}:5432/adventureworks')
        print(f'Importing rows {rows_imported} to {rows_imported + len(data_frame)} for table {table_name}')
        data_frame.to_sql(f'stg_{table_name}', postgres_engine, if_exists='replace', index=False)
        rows_imported += len(data_frame)
        print("Data import successful")
    except Exception as error:
        print("Data load error: " + str(error))

try:
    # Call the extract_and_load function
    extract_and_load()
except Exception as error:
    print("Error while processing data: " + str(error))
