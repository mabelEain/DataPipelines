from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    copy_json = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    "

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"Selected table: {self.table} & S3 path: {s3_path}")
        
        if self.table == "staging_events":
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        elif self.table == "staging_songs":   
            s3_path = "s3://{}/{}/A/A/A".format(self.s3_bucket, rendered_key)
        
        
        formatted_sql = self.copy_json.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            'auto')
        self.log.info(f"Table {self.table} Formatted sql : {formatted_sql}")

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift.run(formatted_sql)
        self.log.info(f"Table {self.table} completed!!")
