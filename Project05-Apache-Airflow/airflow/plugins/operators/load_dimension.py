from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate=False,
                 insert_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.insert_sql = insert_sql

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        
        self.log.info("Getting credentials for loading dimension table: {}".format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info("Truncate before loading table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Loading data into Redshift dimension table: {}".format(self.table))
        sql_load_dimension = "INSERT INTO {} {}".format(self.table, self.insert_sql)
        redshift.run(sql_load_dimension)
        self.log.info("Finished loading data into Redshift dimension table: {}".format(self.table))
