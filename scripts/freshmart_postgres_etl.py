# Part B: Data Storage with Postgresql

#Establishing a PostgreSQL connection using psycopg2

#Establishing a PostgreSQL connection using psycopg2

import psycopg2

#to connect to postgres
def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = '10Alytics_Freshmart',
        user = 'postgres',
        password = '',
        port = '5432'
    )
    return connection



#create table in sql
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    print("Connection to database established successfully")
    create_table_query = """
                            CREATE SCHEMA IF NOT EXISTS freshmart;
                            
                            DROP TABLE IF EXISTS freshmart.products1;
                            
                            CREATE TABLE IF NOT EXISTS freshmart.products1 (
                                ProductID SERIAL PRIMARY KEY,
                                ProductName TEXT,
                                Category TEXT,
                                Price FLOAT,
                                StockQuantity INTEGER
                            );
                         """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    print("Table created successfully")


def load_data(df):
    conn = get_db_connection()
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""INSERT INTO freshmart.products1 (ProductID, ProductName, Category, Price, StockQuantity)
                    VALUES (%s, %s, %s, %s, %s);""", (row['ProductID'], row['ProductName'], row['Category'], row['Price'], row['StockQuantity']))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
print("Records inserted successfully!")


def load_data(df):
    conn = get_db_connection()
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""INSERT INTO freshmart.products1 (ProductID, ProductName, Category, Price, StockQuantity)
                    VALUES (%s, %s, %s, %s, %s);""", (row['ProductID'], row['ProductName'], row['Category'], row['Price'], row['StockQuantity']))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
