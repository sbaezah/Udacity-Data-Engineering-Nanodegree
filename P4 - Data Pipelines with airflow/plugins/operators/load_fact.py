from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
        This script loads data into the fact table
        
        Input parameters
            redshift_conn_id: the name of the connection that should be configured in airflow, previously of the DAG execution.
            
            destination_table: name of the destination table to insert/update data. This table should be created previously of the DAG execution.
            
            sql_statement: This is a 'select' query that gets data from staging tables and loads the extracted data into the destination table (fact table).
        
        
        Output: Nothing.
    """
  
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_statement=sql_statement

    def execute(self, context):
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Starting: Load fact table')
        insert_query = 'INSERT INTO {} ({})'.format(self.destination_table, self.sql_statement)
        redshift.run(insert_query)
        self.log.info('Finished: Load fact table')
