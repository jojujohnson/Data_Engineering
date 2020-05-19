import configparser
import psycopg2
from create_tables import drop_table_sql_list, create_table_sql_list

def drop_tables(cur, conn):
    """
        Perform Drop table operations
    """
    print ("DROP ALL TABLES : STARTED")
    for query in drop_table_sql_list:
        cur.execute(query)
        conn.commit()
    print ("DROP ALL TABLES : COMPLETED")

def create_tables(cur, conn):
    """
        Perform Create table operations
    """    
    print ("CREATE ALL TABLES : STARTED")
    for query in create_table_sql_list:
        cur.execute(query)
        conn.commit()
    print ("CREATE ALL TABLES : COMPLETED")

def main():
    """
        Main Function
    """       
    config = configparser.ConfigParser()
    config.read('/home/workspace/airflow/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    
     