import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
         This function iterates over a list containing the variables of SQL queries within sql_queries.py
         and runs each one. In this case, this function will drop all tables.
    """
    print("Dropping any existing tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function iterates over a list containing the variables of SQL queries within sql_queries.py
        and runs each one. In this case, this function will create the staging and dimensional tables.
    """
    print("Creating dimensional and staging tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Reads the variables within the dwh.cfg file and uses it to connect to the Postgresql database.
        A cursor is created that encapsulates the entire SQL query to process each individual row from the result
        set at a time.

        Finally, we close the connection and the cursor.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connecting to database: {}\n".format(conn))

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Completed creating all tables!")

    conn.close()


if __name__ == "__main__":
    main()