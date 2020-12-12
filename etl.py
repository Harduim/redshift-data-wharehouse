from sql_queries import copy_table_queries, insert_table_queries
from sqlalchemy import create_engine
from json import loads


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)


if __name__ == "__main__":
    with open("dwh.json") as cf:
        config = loads(cf.read())

    uri = "redshift+psycopg2://{user}:{password}@{host}:{port}/{db}".format(**config["redshift"])
    conn = create_engine(uri)
