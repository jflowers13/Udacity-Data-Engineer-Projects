from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql

    def execute(self, context):
        # self.log.info('LoadFactOperator not implemented yet')
        
        self.log.info("Getting credentials for loading fact table: {}".format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into Redshift fact table: {}".format(self.table))
        sql_load_fact = "INSERT INTO {} {}".format(self.table, self.insert_sql)
        redshift.run(sql_load_fact)
        self.log.info("Finished loading data into Redshift fact table: {}".format(self.table))
