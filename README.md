# DuckDB-Python
DuckDB is an in-process SQL OLAP database management system, designed to support analytical queries on large datasets efficiently. Its integration with Python, known as DuckDB-Python, allows users to leverage its capabilities directly within Python scripts or interactive environments like Jupyter notebooks.

It integrates well with Python, allowing you to use SQL queries directly on your Python data structures, such as Pandas DataFrames, and supports various data formats like CSV and Parquet.

Here’s a quick guide on how to use DuckDB with Python:

### Installation

First, you need to install DuckDB. You can do this using pip:

```bash
pip install duckdb
```

### Basic Usage

1. **Import DuckDB in your Python script:**

```python
import duckdb
```

2. **Create a connection to an in-memory database:**

```python
con = duckdb.connect()
```

3. **Create a DataFrame and execute SQL queries on it:**

```python
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

# Register DataFrame as a table
con.register('my_table', df)

# Execute SQL query
result = con.execute("SELECT * FROM my_table WHERE age > 25").fetchdf()

print(result)
```

4. **Reading and writing data to/from CSV or Parquet files:**

```python
# Writing to a Parquet file
con.execute("COPY (SELECT * FROM my_table) TO 'my_table.parquet' (FORMAT 'parquet')")

# Reading from a Parquet file
df_from_parquet = con.execute("SELECT * FROM 'my_table.parquet'").fetchdf()

print(df_from_parquet)
```

5. **Using DuckDB with Pandas:**

```python
# Create a Pandas DataFrame from a SQL query
result_df = con.execute("SELECT * FROM my_table").fetchdf()
print(result_df)
```

### Advanced Usage

- **Joins and aggregations:**

```python
# Sample DataFrame
df2 = pd.DataFrame({
    'name': ['Alice', 'Bob', 'David'],
    'salary': [50000, 60000, 70000]
})

# Register the second DataFrame
con.register('salary_table', df2)

# Perform a join
result = con.execute("""
    SELECT my_table.name, my_table.age, salary_table.salary
    FROM my_table
    JOIN salary_table
    ON my_table.name = salary_table.name
""").fetchdf()

print(result)
```

- **Using DuckDB with other data formats:**

DuckDB can directly read and write various data formats, including CSV, Parquet, and JSON.

```python
# Reading CSV file
df_from_csv = con.execute("SELECT * FROM 'my_table.csv'").fetchdf()

# Writing to CSV file
con.execute("COPY (SELECT * FROM my_table) TO 'my_table.csv' (FORMAT 'csv')")
```

### Benefits of DuckDB

- **Speed:** Optimized for complex analytical queries.
- **In-Process:** Runs in the same process as your application, minimizing latency.
- **Interoperability:** Integrates well with Pandas, making it easy to work with dataframes.
- **Versatility:** Supports various data formats out of the box.

DuckDB is particularly useful for data analysis tasks where you need the power of SQL combined with the flexibility of Python, especially when dealing with large datasets that might be cumbersome to handle using only Pandas.
