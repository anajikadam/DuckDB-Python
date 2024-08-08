import os, time, json
import duckdb as ddb
import pandas as pd

from python_sql.config import root_path

class SQLConn:
    def __init__(self, db_name):
        """Initialize the database connection and cursor."""
        self._root_path = root_path
        self._dbfdr_path = os.path.join(self._root_path, db_name)
        self.db_name = db_name
        self._conn = self._connect_DB()
        self._cursor = self._create_cursor()
        
    def __str__(self) -> str:
        return "Class SQLConn active."
    
    def _connect_DB(self):
        """Connect to the DuckDB database or create one if it doesn't exist."""
        if not os.path.exists(self._dbfdr_path):
            os.mkdir(self._dbfdr_path)
        db_name = self.db_name
        db_folder_path = self._dbfdr_path
        db_file_path = f"{db_folder_path}/{db_name}.db"
        # check if db_path exixt then load it only
        if os.path.exists(db_file_path):
            conn = ddb.connect(database = db_file_path)
            print(f"Database '{self.db_name}' loaded.")
        else:
            conn = ddb.connect(database = db_file_path)
            print(f"New Database '{self.db_name}' created and loaded successfully.")
        return conn
        
    def _create_cursor(self):
        """Create a cursor object to interact with the database."""
        cursor = self._conn.cursor()
        return cursor
    
    def show_tables(self):
        """Display all tables in the database."""
        self._cursor.execute("""SHOW TABLES;""")
        rows = self._cursor.fetchall()
        if not rows:
            print("No table available.")
        else:
            print(f"Number of tables: {len(rows)}")
            for row in rows:
                print(row)
        self.close_conn_db()

    def export_db_csv(self, dir_name):
        self._cursor.execute(f"EXPORT DATABASE '{dir_name}'") 
        self.close_conn_db()   

    def execute_query(self, query):
        self._cursor.execute(query)        

    def close_conn_db(self):
        """Close the database connection."""
        self._conn.close()
        self._cursor = None
        print("\n\t\tConnection Disconnected.")

class UpdateTable(SQLConn):
    def __init__(self, db_name, table_name):
        """Initialize the database and table name."""
        super().__init__(db_name)
        self.table_name = table_name
    
    def create_table(self, schema):
        """Create a table with the specified schema if it doesn't exist."""
        query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} {schema}"""
        self._cursor.execute(query)
        print(f"Table {self.table_name} created successfully.")
        self.close_conn_db()
        
    def truncate_table(self):
        query = f"""TRUNCATE TABLE {self.table_name};"""
        self._cursor.execute(query)
        print(f"TRUNCATE table {self.table_name}")
        
    def drop_table(self):
        query = f"""DROP TABLE IF EXISTS {self.table_name};"""
        self._cursor.execute(query)
        print(f"DROP table {self.table_name}")

    def insert_single_data_into_table(self, data_tuple):
        """Insert a single row of data into the table."""
        query = f"""INSERT INTO {self.table_name} VALUES {data_tuple}"""
        self._cursor.execute(query)
        print(f"Single row inserted into table {self.table_name} successfully.")
        self.close_conn_db()

    def insert_csv_file_data_into_table(self, csv_file_path):
        """Insert data from csv file into the table."""
        query = f"COPY my_courses FROM '{csv_file_path}' (FORMAT 'csv')"
        # Load data from CSV into DuckDB table
        self._cursor.execute(query)
        print(f"Data from csv {csv_file_path} inserted into table {self.table_name} successfully.")
        self.close_conn_db()
   
class ShowTable(SQLConn):
    def __init__(self, db_name, table_name):
        """Initialize the database and table name."""
        super().__init__(db_name)
        self.table_name = table_name
        
    def show_table_data_all(self):
        """Display all data from the table."""
        query = f'SELECT * FROM {self.table_name}'
        self._cursor.execute(query)
        rows = self._cursor.fetchall()
        print(f"Number of rows: {len(rows)}")
        for row in rows:
            print(row)
        self.close_conn_db()

    def table_data_to_df(self):
        """Fetch all data from the table and return as a pandas DataFrame."""
        query = f'SELECT * FROM {self.table_name}'
        self._cursor.execute(query)
        rows = self._cursor.fetchall()
        df = pd.DataFrame(rows)
        print(f"Number of rows: {len(rows)}")
        self.close_conn_db()
        return df

