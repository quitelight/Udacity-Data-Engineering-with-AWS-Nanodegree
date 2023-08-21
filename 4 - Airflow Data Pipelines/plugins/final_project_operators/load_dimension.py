from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 query,
                 table,
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Connecting to Amazon Redshift")
        redshift = PostgresHook(postgres_conn_id = conn_id)

        if self.truncate:
            self.log.info("Begin truncate of table prior to insertion of new data")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Load data from Amazon S3 to Redshift")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        self.log.info(f"Completed inserting data into {self.table}")
