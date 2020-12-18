from sql_queries import (
    insert_dim_table_queries,
    run_queries_parallel,
    run_queries_sequential,
    songplay_table_insert,
    staging_copy_queries,
    time_table_insert,
)

if __name__ == "__main__":
    print("Importing JSON files to staging area")
    run_queries_parallel(staging_copy_queries)

    print("Populating dimention tables")
    run_queries_parallel(insert_dim_table_queries)

    print("Populating songplays and time")
    run_queries_sequential([songplay_table_insert, time_table_insert])
