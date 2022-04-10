from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_cmd = """
            COPY {redshift_table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{aws_access_key}'
            SECRET_ACCESS_KEY '{aws_secret_key}'
            REGION AS '{s3_region}'
            FORMAT as json '{s3_json_format}'
        """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key="",
                 copy_json_option = "auto",
                 region = "",               
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info("Truncating table {table_name}".format(table_name = self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        parsed_key = self.s3_key.format(**context)
#         s3_path = "s3://{self.s3_bucket}/{parsed_key}"
#         s3_path = 's3://' + 'self.s3_bucket' + '/' + parsed_key
        s3_path = "s3://{}/{}".format(self.s3_bucket, parsed_key)
    
        aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
        formatted_cmd = StageToRedshiftOperator.copy_cmd.format(
            redshift_table = self.table,
            s3_path = s3_path,
            aws_access_key = aws_hook.get_credentials().access_key,
            aws_secret_key = aws_hook.get_credentials().secret_key,  
            s3_region = self.region,
            s3_json_format = self.copy_json_option
        )
        redshift.run(formatted_cmd)

