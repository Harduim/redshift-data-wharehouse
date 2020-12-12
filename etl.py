from sql_queries import copy_table_queries, insert_table_queries, get_connection
from json import loads


def load_staging_tables(cur):
    for query in copy_table_queries:
        cur.execute(query)


def insert_tables(cur):
    for query in insert_table_queries:
        cur.execute(query)


if __name__ == "__main__":
    with open("dwh.json") as cf:
        config = loads(cf.read())

    conn = get_connection(config["redshift"])
