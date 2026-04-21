import random
import string
from datetime import datetime, timedelta

def infer_sql_type(col):
    col = col.lower()
    if "date" in col or "time" in col:
        return "DATETIME"
    if "id" in col:
        return "INT"
    if "amount" in col or "price" in col:
        return "FLOAT"
    if "flag" in col or "is_" in col:
        return "BIT"
    return "NVARCHAR(255)"


def generate_value(col, sql_type):
    if sql_type == "INT":
        return random.randint(1, 1000)
    if sql_type == "FLOAT":
        return round(random.uniform(10, 10000), 2)
    if sql_type == "BIT":
        return random.choice([0, 1])
    if sql_type == "DATETIME":
        return datetime.now() - timedelta(days=random.randint(0, 365))
    return ''.join(random.choices(string.ascii_uppercase, k=8))

import pandas as pd
import pyodbc

EXCEL_FILE = "../data/pbi data.xlsx"
SCHEMA = "dbo"
ROWS_PER_TABLE = 100  # change for more/less variation

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=dcc;'
        'Trusted_Connection=yes;'
)
cursor = conn.cursor()

xls = pd.ExcelFile(EXCEL_FILE)

for sheet in xls.sheet_names:
    df = pd.read_excel(xls, sheet)

    table_name = sheet.replace(" ", "_")
    columns = df.columns.tolist()

    if not columns:
        print(f"⚠ Skipping empty sheet: {table_name}")
        continue

    # -------- CREATE TABLE --------
    col_defs = []
    col_types = {}

    for col in columns:
        sql_type = infer_sql_type(col)
        col_defs.append(f"[{col}] {sql_type}")
        col_types[col] = sql_type

    create_sql = f"""
    IF OBJECT_ID('{SCHEMA}.{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE [{SCHEMA}].[{table_name}] (
            {",".join(col_defs)}
        )
    END
    """
    cursor.execute(create_sql)
    conn.commit()

    # -------- INSERT RANDOM DATA --------
    insert_sql = f"""
    INSERT INTO [{SCHEMA}].[{table_name}]
    ({",".join(f"[{c}]" for c in columns)})
    VALUES ({",".join("?" for _ in columns)})
    """

    for _ in range(ROWS_PER_TABLE):
        row = [generate_value(col, col_types[col]) for col in columns]
        cursor.execute(insert_sql, row)

    conn.commit()
    print(f"✔ Created & populated table: {table_name}")

cursor.close()
conn.close()
