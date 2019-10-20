from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ This script is called by the operator that load the data from staging tables to the dimension tables.
        
        Input Parameters:
            redshift_conn_id: the name of the connection that should be configured in airflow, previously of the DAG execution
            
            destination_table: name of the destination table to insert/update data. This table should be created previously of the DAG execution.
            
            sql_statement: Is a 'select' query that gets data from staging tables and load into the destination table (dimensions). The pre-requisite is that the table should be exists.
            
            update_mode: the user have two options: 'insert' or 'overwrite'. The 'insert' option just insert rows in the table, the 'overwrite' optin, truncates the destination table and then insert the rows.
            
         
     Output:
            For the moment this script returns nothing. But, if the user give an input different of the two possible options, the class writes a message in the log.
    
    """
   

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql_statement = "",
                 update_mode = "overwrite",  # insert, overwrite
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_statement=sql_statement
        self.update_mode=update_mode

    def execute(self, context):
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Loading dimension table {}'.format(self.destination_table))
        if self.update_mode == 'overwrite':
            update_query = 'TRUNCATE {}; INSERT INTO {} ({})'.format(self.destination_table, self.destination_table, self.sql_statement)
        elif self.update_mode == 'insert':
            update_query = 'INSERT INTO {} ({})'.format(self.destination_table, self.sql_statement)
        redshift.run(update_query)
        else:
            self.log.info('Option selected does not exists')
       
