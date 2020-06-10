from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CsvToRedshiftOperator(BaseOperator):
    name="csv_to_redshift"
    template_fields = ("s3_key",)
    copy_sql = """
        copy {}
        FROM '{}'
        IAM_ROLE '{}'
        CSV
        IGNOREHEADER 1
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 iam_role = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 *args, **kwargs):

         super(CsvToRedshiftOperator, self).__init__(*args, **kwargs)
         self.table = table
         self.redshift_conn_id = redshift_conn_id
         self.s3_bucket = s3_bucket
         self.s3_key = s3_key
         self.iam_role = iam_role

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

       # self.log.info("Clearing data from destination Redshift table")
       # redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying csv data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = CsvToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                self.iam_role
                )
         
        redshift.run(formatted_sql)
