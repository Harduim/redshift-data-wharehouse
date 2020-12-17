from json import loads

from sql_queries import create_table_queries, drop_table_queries, get_engine, run_queries

if __name__ == "__main__":
    with open("dwh.json") as cf:
        config = loads(cf.read())

    eng = get_engine(config["redshift"])

    # Using connect to keep everything in one transaction
    with eng.connect() as con:
        run_queries(con, drop_table_queries)
        run_queries(con, create_table_queries)
