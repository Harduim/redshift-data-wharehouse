from sql_queries import create_table_queries, drop_table_queries, run_queries_sequential

if __name__ == "__main__":
    run_queries_sequential(drop_table_queries)
    run_queries_sequential(create_table_queries)
