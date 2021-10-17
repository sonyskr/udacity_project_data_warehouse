import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables from sparkifydb.
    Input Arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (sparkifydb).
    Output Return:
    * None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create new tables (songplays, users, artists, songs, time)
        to sparkifydb.
    Input arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (sparkifydb).
    Output Return:
    * None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to AWS Redshift, create new DB (sparkifydb),
        drop any existing tables, create new tables. Close DB connection.
    Input arguments (from dwh.cfg):
    * None
    Output Return:
    * None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()