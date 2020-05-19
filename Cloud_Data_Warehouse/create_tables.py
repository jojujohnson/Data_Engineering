import configparser
import psycopg2
from sql_queries import drop_table_sql_list, create_table_sql_list


def drop_tables(cur, conn):
    print ("DROP ALL TABLES : STARTED")
    for query in drop_table_sql_list:
        cur.execute(query)
        conn.commit()
    print ("DROP ALL TABLES : COMPLETED")

def create_tables(cur, conn):
    print ("CREATE ALL TABLES : STARTED")
    for query in create_table_sql_list:
        cur.execute(query)
        conn.commit()
    print ("CREATE ALL TABLES : COMPLETED")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    
     