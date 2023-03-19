create_employees_table = PostgresOperator(
    task_id="create_employees_table",
    postgres_conn_id="tutorial_pg_conn",
    sql="sql/employees_schema.sql",
)