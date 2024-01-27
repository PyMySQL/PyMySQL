#!/usr/bin/env python
import pymysql

def connect_to_database(host="localhost", port=3306, user="root", passwd="", db="mysql"):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
        return conn
    except pymysql.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def fetch_and_print_user_data(conn):
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT Host, User FROM user")
            print(cur.description)
            print()

            for row in cur:
                print(row)

    except pymysql.Error as e:
        print(f"Error executing SQL query: {e}")

def main():
    db_connection = connect_to_database()
    fetch_and_print_user_data(db_connection)

    if db_connection:
        db_connection.close()

if __name__ == "__main__":
    main()
