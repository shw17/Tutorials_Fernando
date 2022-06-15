import time
import typing
from urllib.parse import urlparse

import boto3
import pandas as pd


class AthenaDAO:
    def __init__(self, region, database, bucket, workgroup="etl"):
        self.params = {
            "region": region,
            "database": database,
            "bucket": bucket,
            "workgroup": workgroup,
            "path": "tmp/athena",  # this is where querying outputs land
        }

    def __athena_query(self, client, params):
        response = client.start_query_execution(
            QueryString=params["query"],
            QueryExecutionContext={"Database": params["database"]},
            WorkGroup=params["workgroup"]
        )
        return response

    def __execute(self, params, max_execution=1000):
        client = boto3.client("athena", region_name=params["region"])
        execution = self.__athena_query(client, params)
        execution_id = execution["QueryExecutionId"]
        state = "RUNNING"
        duration = 300
        while max_execution > 0 and state in ["RUNNING", "QUEUED"] and duration > 0:  # noqa
            max_execution = max_execution - 1
            response = client.get_query_execution(QueryExecutionId=execution_id)  # noqa
            if (
                "QueryExecution" in response and
                "Status" in response["QueryExecution"] and
                "State" in response["QueryExecution"]["Status"]
            ):
                state = response["QueryExecution"]["Status"]["State"]
                if state == "FAILED":
                    reason = response["QueryExecution"]["Status"]["StateChangeReason"]
                    raise Exception(reason)
                elif state == "SUCCEEDED":
                    s3_path = response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]  # noqa
                    return s3_path
            duration = duration - 5
            time.sleep(5)

    def execute(self, query, download=False):
        params = self.params  # getting copy to remain stateless
        params["query"] = query  # adding query to copy
        s3_path = self.__execute(params)
        if download is True:
            s3_client = boto3.client("s3")
            url_parsed = urlparse(s3_path, allow_fragments=False)
            response = s3_client.get_object(Bucket=url_parsed.netloc, Key=url_parsed.path[1:])
            data = pd.read_csv(response.get("Body"))
            return data

    def add_partition(  # pylint: disable=too-many-arguments
        self, table_name: str, partitions: typing.Dict, location: str, bucket=None, if_not_exists=False
    ) -> None:
        partitions = ", ".join([f"{k} = '{v}'" for k, v in partitions.items()])
        if_not_exists = "IF NOT EXISTS" if if_not_exists else ""
        bucket = bucket if bucket is not None else self.params["bucket"]

        query = f"""
        ALTER TABLE {table_name}
            ADD {if_not_exists} PARTITION ({partitions})
        LOCATION 's3://{bucket}/{location}'
        """
        self.execute(query)
