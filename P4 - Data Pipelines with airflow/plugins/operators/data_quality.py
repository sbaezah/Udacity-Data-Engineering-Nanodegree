from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """ 
        This script calls the data quality checks, that were created in the data_quality_checks.py file.
        
        
        Input Paramaters:
            data_check_query: a list with one or more queries that applies data checks
            
            table: a list with at least one table name (can be more than one), that will be checked with the data check process.
            
            expected_results: a list with the values that the user expect as results of the data check process
        
        
        Output
        
         The process returns an exception when the data check fails.        
    
    """
    
  

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_check_query=[],
                 table=[],
                 expected_result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.data_check_query=data_check_query
        self.table=table
        self.expected_result=expected_result

    def execute(self, context):
        self.log.info('Running data quality checks')
        
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        checks = zip(self.data_check_query, self.table, self.expected_result)
        for check in checks:
            try:
                redshift.run(check[0].format(check[1])) == check[2]
                self.log.info('Data quality check status: PASSED.')
            except:
                self.log.info('Data quality check status: FAILED.')
                raise AssertionError('Data quality check status: FAILED.')