'''
    This file contains functions to create and drop database schemas, and tables
'''
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_schemas(cur, conn):
    '''
        Function to create schemas. This function uses the variable 'create_schemas_queries' defined in the 'sql_queries.py' file.
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    for query in create_schemas_queries:
        cur.execute(query)
        conn.commit()  
        

def drop_schemas(cur, conn):
    '''
        Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    for query in drop_schemas_queries:
        cur.execute(query)
        conn.commit()
        
        
def create_tables(cur, conn):
    '''
        Function to create tables. This function uses the variable 'create_table_queries' defined in the 'sql_queries.py' file.
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    '''
        Function to drop tables. This function uses the variable 'drop_table_queries' defined in the 'sql_queries.py' file.
        Parameters:
            - curr: Cursor for a database connection
            - conn: Database connection
        Outputs:
            None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()





def main():
    '''
        Function connects to the database, parses the configurations from 'dwh.cfg' file to create, and drop schemas & tables. 
        The functions 'drop_schemas', 'create_schemas','drop_tables','create_tables' defined in this file were used to performs the
        drop and create tables tasks, and lastly close the connection to the database
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
    
    drop_schemas(cur, conn)
    create_schemas(cur, conn)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()