'''
    This file contains functions to extract data from Udacity S3 bucket into staging tables, 
    Tansform and load the staged data into the dimension and fact tables.
'''
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
        Function to extract data from Udacity S3 bucket to staging tables (songs & events)
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
        Function to transform and load data from staging tables (songs & events)
        into the dimension (users, songs, artists, time) and fact (songplays) tables 
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
        Function connects to the database, parses the configurations from 'dwh.cfg' file to Extract data from S3 bucket int the 
        staging tables and then from staging tables transform the data and insert into the dimension and fact tables. 
        And lastly close the connection to the database
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()