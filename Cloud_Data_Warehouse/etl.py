import configparser
import psycopg2
from sql_queries import staging_table_sql_list, insert_table_sql_list


def load_staging_tables(cur, conn):
    print ("STAGING TABLES DATA LOAD : STARTED")
    for query in staging_table_sql_list:
        cur.execute(query)
        conn.commit()
    print ("STAGING TABLES DATA LOAD : COMPLETED")

def load_dimension_fact_tables(cur, conn):
    print ("DIMENSION and FACT TABLES DATA LOAD : STARTED")
    for query in insert_table_sql_list:
        cur.execute(query)
        conn.commit()
    print ("DIMENSION and FACT TABLES DATA LOAD : COMPLETED")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    load_dimension_fact_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()