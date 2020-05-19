from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
    LoadFactOperator creates a node in the dag  to Load fact table in Redshift using data in staging tables
"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 # Define operators params 
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params defintion
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        # Perform Operation
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into fact table in Redshift")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift_hook.run(table_insert_sql)