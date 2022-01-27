from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        self.log.info("Starting Data Quality Checks")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info("Running Data Quality Check on table: {}".format(table))
            
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            
            if records is None or len(records[0]) < 1:
                logging.error("No records present in table: {}".format(table))
                raise ValueError("No records present in table: {}".format(table))
                
            self.log.info("Data Quality check passed with {} records in table: {}".format(records[0][0], table))
            
        