import json
from io import StringIO

import boto3


class AWSS3:
    """Class contains common logic for working with AWS S3"""

    def __init__(self):
        self.__s3_resource = boto3.resource("s3")

    def save_as_csv(self, df, bucket, path, **kwargs):
        buffer = StringIO()
        df.to_csv(buffer, **kwargs)

        self.__s3_resource.Object(bucket, path).put(Body=buffer.getvalue())

    def save_as_json(self, df, bucket, path, **kwargs):
        buffer = StringIO()
        records = df.to_dict(orient="records")
        for record in records:
            json.dump(record, buffer)
            buffer.write("\n")

        self.__s3_resource.Object(bucket, path).put(Body=buffer.getvalue())
