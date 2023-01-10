import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, insert_table_info, staging_table_info, sample_queries, sample_query_info


def load_staging_tables(cur, conn):
    """
        This function iterates over a list containing the variables of SQL queries within sql_queries.py
        and runs each one.
        There's another list it uses called staging_table_info, this contains headings of what the function is
        currently performing so the user understands the stage of the load.
        Here we pull in both arrays as a tuple and then unpack it in the for loop.
    """
    print("Beginning loading of data from S3 into the staging tables...")
    for query, info in list(zip(copy_table_queries, staging_table_info)):
        print("Currently loading the {} from S3 to the staging table...".format(info))
        cur.execute(query)
        conn.commit()
    print("Completed loading all data into the staging tables!")


def insert_tables(cur, conn):
    """
        This function iterates over a list containing the variables of SQL queries within sql_queries.py
        and runs each one.
    """
    print("Beginning loading of dimensional data from staging to production analytics tables...")
    for query, info in list(zip(insert_table_queries, insert_table_info)):
        print("Loading data from {}...".format(info))
        cur.execute(query)
        conn.commit()
    print("Completed inserting data into the dimensional tables!")

def run_sample_queries(cur, conn):
    """
        This function iterates over a list within sql_queries.py containing the variables of sample SQL queries to be
        run after all data load has completed.
      """
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
    """
        Reads the variables within the dwh.cfg file and uses it to connect to the Postgresql database.
        A cursor is created that encapsulates the entire SQL query to process each individual row at a time.
        Hence, for the run_sample_queries() function we've set up a while loop to iterate over each row and fetch it.

        Finally, we close the connection and the cursor.
    """
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