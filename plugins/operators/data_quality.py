from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 check_datas = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id              
               
        if check_datas is None:
            check_datas = []
        self.check_datas = check_datas
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):     
#         sql_count = """SELECT COUNT(*) FROM {};""".format(check_data)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)     
        for check_data in self.check_datas:
            raise ValueError("Test result failed {}".format( check_data))
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(check_data))
            raise ValueError("Test result failed {}".format( records))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Test result failed")
            num_records = records[0][0]
            
            if num_records == 0:
                raise ValueError("No records were recorded")           
            self.log.info("Get {} records on table {}".format( num_records,  check_data))