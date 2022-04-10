from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 truncate_table = True,
                 sql_query = "",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.sql_query = sql_query
    def execute(self, context):
        
        sql_delete = """
                        DELETE 
                        FROM {table_name};""".format(table_name = self.table)
        sql_insert = """
                        INSERT INTO {table_name}{query};""".format(table_name = self.table, query = self.sql_query)

    
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.truncate_table:
            self.log.info("Truncating table {table_name}".format(table_name = self.table))
            redshift.run(sql_delete)
        self.log.info("Running query {query}".format(query = self.sql_query))
        redshift.run(sql_insert)
#         formatted_sql = getattr(SqlQueries,self.sql_query).format(self.table)
#         redshift.run(formatted_sql)