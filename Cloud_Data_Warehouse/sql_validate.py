import configparser
import psycopg2
from sql_queries import row_count_sql_list


def get_results(cur, conn):
    """
    Get row count from all tables
    """
    for query in row_count_sql_list:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)


def main():
    """
    Execute queries to confirm on data load 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    get_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    