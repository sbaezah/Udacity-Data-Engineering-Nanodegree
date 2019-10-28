from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
       This script gets data from S3 buckets and inserts it into the staging tables.
        
        INPUT PARAMETERS:
            aws_credentials_id: Connection Id of the Amazon Web services that should be created in airflow before execute the DAG
            
            redshift_conn_id: Connection Id of the Redshift cluster that should be created in Airflow before de DAG execution
            
            table: name of the destination table, in this operator the input table will be a staging table.
            
            s3_bucket: the S3 bucket that will be used in the process, for this process the public bucket should be 'udacity-dend'
            
            s3_key: S3 key that will be used, it is like a 'name folder', eg. 'log_data/'.
            
            data_format: the user can choose between two: 'csv' or 'json'.
            
            delimiter: if the user selects 'csv' as data_format, it can put the delimiter for the csv file
            
            ignore_headers: the user has two options: '0' or '1'
            
            data_format: the user can choose between 'json' or 'csv', when it try with 'csv', probably will need the ignore_headers parameter setted in '1'.
            
            jsonpaths: this input is for the folder and file of 'JSON paths' file
            
            
    Output:
        there is no one output from this script
        
    """
    
    template_fields = ("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1, #
                 data_format="csv",
                 jsonpaths="",
                 *args, **kwargs):

        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.s3_bucket=s3_bucket # udacity-dend
        self.s3_key=s3_key # log_data, song_data
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.data_format=data_format.lower()     # 'csv', 'json'
        self.jsonpaths=jsonpaths
        
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        
        # select csv or json format
        if self.data_format == 'csv':
            autoformat = "DELIMITER '{}'".format(self.delimiter)
        elif self.data_format == 'json':
            json_option = self.jsonpaths
            autoformat = "FORMAT AS JSON '{}'".format(json_option)
        
        # set S3 path based on execution dates  
        rendered_key = self.s3_key.format(**context)
        self.log.info('Rendered key is ' + rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            autoformat
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)





