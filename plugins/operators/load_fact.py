from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    truncate_query = """
        TRUNCATE TABLE {table}
    """
    insert_query = """
        INSERT INTO {table} 
        {select_query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query_name="",
                 truncate_table=False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query_name = sql_query_name
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f'LoadFactOperator in progress for table: {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating fact table: {self.table}")
            redshift.run(LoadFactOperator.truncate_query.format(
                table=self.table))
        self.log.info(f"Inserting data into fact table {self.table}")
        select_sql_query = getattr(SqlQueries, self.sql_query_name)
        redshift.run(LoadFactOperator.insert_query.format(
            table=self.table, select_query=select_sql_query))
