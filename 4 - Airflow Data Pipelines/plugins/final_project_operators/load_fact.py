from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 query,
                 table,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = query
        self.table = table

    def execute(self, context):
        self.log.info("Connecting to Amazon Redshift")
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        self.log.info("Insert data from Amazon S3 to Redshift")
        redshift.run(f"INSERT INTO {self.table} {self.query}")