import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, insert_table_info, staging_table_info, sample_queries, sample_query_info


def load_staging_tables(cur, conn):
    print("Beginning loading of data from S3 into the staging tables...")
    for query, info in list(zip(copy_table_queries, staging_table_info)):
        print("Currently loading the {} from S3 to the staging table...".format(info))
        cur.execute(query)
        conn.commit()
    print("Completed loading all data into the staging tables!")


def insert_tables(cur, conn):
    print("Beginning loading of dimensional data from staging to production analytics tables...")
    for query, info in list(zip(insert_table_queries, insert_table_info)):
        print("Loading data from {}...".format(info))
        cur.execute(query)
        conn.commit()
    print("Completed inserting data into the dimensional tables!")

def run_sample_queries(cur, conn):
    print("Run some sample queries as a test...")
    for query, info in list(zip(sample_queries, sample_query_info)):
        print("{}".format(info))
        cur.execute(query)
        # Display each row result from the cursor
        row = cur.fetchone()
        while row:
            print(list(row))
            row = cur.fetchone()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_sample_queries(cur, conn)
    print("Completed all operations!")

    conn.close()


if __name__ == "__main__":
    main()