import configparser
import psycopg2
from json import loads
from sql_queries import get_connection, create_table_queries, drop_table_queries


def drop_tables(cur):
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur):
    for query in create_table_queries:
        cur.execute(query)


if __name__ == "__main__":
    with open("dwh.json") as cf:
        config = loads(cf.read())

    conn = get_connection(config["redshift"])

    with conn.connect() as cur:
        drop_tables(cur)
        create_tables(cur)
