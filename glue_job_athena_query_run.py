import ssl
import botocore
import numpy as np
from typing import Callable, Optional
import alerts
import boto3
import aws_glue_cec
from awsglue.utils import getResolvedOptions
import sys
import json


def _ddl_adjustment_of_metadata(database_raw = 'glue-7214-development-db-raw',
                                athenaAlterSQLQuery = None,
                                workgroup = 'athena-7214-development-workgroup'):
    """
    RAW Data, in csv files, doesn't have, by default, the right metadata/column names when sent to AWS Aurora. 
    For now, we fix this in AWS Athena by running, first, an ALTER TABLE/REPLACE COLUMNS query to appropriatly name columns
    and, second, an ALTER TABLE/SET TBLPROPERTIES query to remove the first row of the file, which are the aforementioned columns names.
    This function thus takes two variables, the first SQL query, in string type (be very careful with quotation marks), 
    and the second SQL query, also in string type and also careful with quotation marks.
    """
    
    #Connecting to AWS Athena
    athena_client = boto3.client('athena')

    #Executing our altering SQL query in AWS Athena
    athena_client.start_query_execution(
        QueryString = athenaAlterSQLQuery,
        QueryExecutionContext = {
            'Database': database_raw},
        WorkGroup = workgroup)


def _extract_tables_and_alter_queries_in_athena():
    '''
    Go into the JSON with Raw table informations, retrieve our alter table queries for AWS Athena, and run these queries.
    '''

    #Connecting to AWS S3
    s3_resource = boto3.resource("s3")

    # Extract values from JSON in S3
    args = getResolvedOptions(sys.argv, ["TABLE_LIST_BUCKET", "TABLE_LIST_PREFIX"])
    table_list_bucket = args["TABLE_LIST_BUCKET"]
    table_list_prefix = args["TABLE_LIST_PREFIX"]
    filename = table_list_prefix.split("/")[-1]
    s3_resource.Bucket(table_list_bucket).download_file(table_list_prefix, filename)
    with open(filename, "r") as reader:
        table_data_raw = reader.read()
    table_data = json.loads(table_data_raw)

    # Iterate through JSON table lists
    for table in range(0, len(table_data["tables"])):
        table_infos = table_data["tables"][table]

        if "alter_sql" in table_infos:
            list_alter_sql_queries = table_infos["alter_sql"]

            for query in list_alter_sql_queries:
                _ddl_adjustment_of_metadata(database_raw = 'glue-7214-development-db-raw',
                                            athenaAlterSQLQuery = query,
                                            workgroup = 'athena-7214-development-workgroup')
                                            
        else:
            pass


def main(aws_glue_processor: Optional[Callable] = None) -> None:
    """
    Main function to execute the current Python Shell
    """
    if aws_glue_processor is None:
        aws_glue_processor = aws_glue_cec.create_processor(
            values_to_extract=VALUES_TO_EXTRACT_FROM_AWS,
            glue_job_name="GuillaumeGirouxTesting",
        )

    aws_glue_processor.glue_job_id = "monitoring"
    #alerts.handle_exceptions(_extract_tables_and_alter_queries_in_athena, aws_glue_processor)


if __name__ == "__main__":

    # Values to extract from AWS shell
    VALUES_TO_EXTRACT_FROM_AWS = {
        "DefaultArguments": ["TABLE_LIST_BUCKET", "TABLE_LIST_PREFIX", "EXECUTION_ID"],
        "Command": ["ScriptLocation"]}

    main()

    _extract_tables_and_alter_queries_in_athena()
  
