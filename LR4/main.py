import psycopg2
import csv
import os

def create_tables(cursor):
    with open('sql/accounts.sql', 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)

    with open('sql/products.sql', 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)

    with open('sql/transactions.sql', 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)

def insert_data(cursor, table_name, csv_file):
    with open(csv_file, 'r') as file:
        csv_reader = list(csv.reader(file))[1:]
        cursor.executemany(f'INSERT INTO {table_name} VALUES ({", ".join(["%s"] * len(csv_reader[0]))})', csv_reader)

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cursor = conn.cursor()

    # Створення нових таблиць
    cursor.execute('DROP TABLE IF EXISTS transactions')
    cursor.execute('DROP TABLE IF EXISTS products')
    cursor.execute('DROP TABLE IF EXISTS accounts')

    create_tables(cursor)

    # додавання даних в таблиці
    insert_data(cursor, 'accounts', 'data/accounts.csv')
    insert_data(cursor, 'products', 'data/products.csv')

    with open('data/transactions.csv', 'r') as file:
        csv_reader = list(csv.reader(file))[1:]
        mydata = [[row[0], row[1], row[2], row[6], row[5]] for row in csv_reader]
        cursor.executemany('''
            INSERT INTO transactions 
                (transaction_id,transaction_date,product_id,account_id,quantity) 
            VALUES (%s, %s, %s, %s, %s)''', mydata)

    # print в консоль вибраних даних
    print("---------accounts---------")
    cursor.execute('SELECT * FROM accounts')
    print(cursor.fetchall())
    print("---------products---------")
    cursor.execute('SELECT * FROM products')
    print(cursor.fetchall())
    print("---------transactions---------")
    cursor.execute('SELECT * FROM transactions')
    print(cursor.fetchall())

    conn.commit()
    cursor.close()
    conn.close()
    print("Script finished successfully")

if __name__ == "__main__":
    main()
