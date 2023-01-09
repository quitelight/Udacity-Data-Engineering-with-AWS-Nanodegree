import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print("Dropping any existing tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    print("Creating dimensional and staging tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #conn = psycopg2.connect("host={} user='dwhuser' password='Passw0rd' port='5439'".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connecting to database: {}\n".format(conn))

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Completed creating all tables!")

    conn.close()


if __name__ == "__main__":
    main()