from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
    LoadDimensionOperator creates a node in the dag to data from S3 to Redshift staging tables.
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):

        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into fact table in Redshift")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift_hook.run(table_insert_sql)
