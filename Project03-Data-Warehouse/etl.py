import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Description:
            Performs the data ingest from the S3 bucket into the staging tables using the queries in the `copy_table_queries` list
        
        Arguments:
            cur: The database cursor object
            conn: The database connection object
            
        Returns:
            None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Description:
            Performs the extraction, transformation and loading of data from the staging tables to the Fact/Dimension tables using the queries in the `insert_table_queries` list
            
        Arguments:
            cur: The database cursor object
            conn: The database connection object
            
        Returns:
            None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # Open and read configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to database and get cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data from S3 bucket into staging tables and then perform ETL from staging tables to Fact/Dimension tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close database connection
    conn.close()


if __name__ == "__main__":
    main()