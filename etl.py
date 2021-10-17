import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load JSON input data (log_data and song_data) from S3 and insert
        into staging_events and staging_songs tables.
    Input arguments:
    * cur --    reference to connected db.
    * conn --   parameters (host, dbname, user, password, port)
                to connect the DB.
    Output:
    * None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables (staging_events and staging_songs)
        into star schema tables
    Input arguments:
    * cur --    reference to connected db.
    * conn --   parameters (host, dbname, user, password, port)
                to connect the DB.
    Output:
    * None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Connect to DB and call
        * load_staging_tables to load data from JSON files in S3 to staging tables
        * insert_tables to insert data to analysis tables.
    Input arguments:
    * None
    Output:
    * None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()