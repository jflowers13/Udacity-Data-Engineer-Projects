import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Description:
            Drops each table using the queries in the `drop_table_queries` list
            
        Arguments:
            cur: The database cursor object
            conn: The database connection object
            
        Returns:
            None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description:
            Creates each table using the queries in the `create_table_queries` list
            
        Arguments:
            cur: The database cursor object
            conn: The database connection object
            
        Returns:
            None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # Open and read configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to database and get cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop tables if they already exist and create new tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close database connection
    conn.close()


if __name__ == "__main__":
    main()