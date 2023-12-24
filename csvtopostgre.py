# Library
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# db connections
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': '***'
}

# connection to the PostgreSQL server
conn = psycopg2.connect(
    host=db_params['host'],
    database=db_params['database'],
    user=db_params['user'],
    password=db_params['password']
)

cur = conn.cursor()

# auto commit=true
conn.set_session(autocommit=True)

# Create db
cur.execute("CREATE DATABASE Foo")

# commit db.create
conn.commit()
cur.close()
conn.close()


# db connect
db_params['database'] = 'annex'
engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}/{db_params["database"]}')

# fullpaths of csvs
csv_files = {
    'foo1': '/Users/[username]/Documents/foo1.csv',
    'foo2': '/Users/[username]/Documents/foo2.csv',
    'foo3': '/Users/[username]/Documents/foo3.csv',
    'foo4': '/Users/[username]/Documents/foo4.csv',
}

# preview
for table_name, file_path in csv_files.items():
    print(f"Contents of '{table_name}' CSV file:")
    df = pd.read_csv(file_path)
    print(df.head(2))
    print("\n")

# importing to pg
for table_name, file_path in csv_files.items():
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)