import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Loading table: '+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Inserting data into table: '+query)
        cur.execute(query)
        conn.commit()


def main():
     
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    dateTimeObj = datetime.now()
    start_time = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    
    print('Starting load staging tables at:' + start_time)
    load_staging_tables(cur, conn)
    
    dateTimeObj2 = datetime.now()
    end_time = dateTimeObj2.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print('loading staging tables ended at:' + end_time)
    
    
    dateTimeObj3 = datetime.now()
    start_time2 = dateTimeObj3.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print('Starting insert data into tables at:' + start_time2)
    insert_tables(cur, conn)
    
    dateTimeObj4 = datetime.now()
    end_time2 = dateTimeObj4.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print('loading staging tables ended at:' + end_time)

    conn.close()


if __name__ == "__main__":
    main()