from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FactDataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
            redshift_conn_id = "",
            tables = [],
            *args, **kwargs):

         super(FactDataQualityOperator, self).__init__(*args, **kwargs)
         self.redshift_conn_id = redshift_conn_id
         self.tables = tables

    def execute(self, context):
        self.log.info(f'FactDataQualityOperator starting with {self.redshift_conn_id}')

        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f'tables: {self.tables}.')
        self.log.info(f'type(tables): {type(self.tables)}.')
        for table in self.tables:
            self.log.info(f'--FactDataQualityOperator checking {table}.')
            self.log.info(f"SELECT COUNT(*) FROM {table};")
            records = redshift.get_records(sql = f"SELECT COUNT(*) FROM {table};")
            if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            self.log.info(f"Data Quality on table {table} check passed with {records[0][0]} records")
