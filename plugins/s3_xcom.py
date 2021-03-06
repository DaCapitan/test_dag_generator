import os
import uuid
import pickle
from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3XComBackend(BaseXCom):
    S3_PREFIX = "airflow-xcom-backend/"
    LOCAL_PREFIX = os.environ["AIRFLOW_HOME"]
    BUCKET_NAME = "b-ws-c48k8-fmm"

    @staticmethod
    def serialize_value(value: Any):
        xcom_type_to_db = (dict, list)

        if not isinstance(value, xcom_type_to_db):
            hook = S3Hook()
            filename = f"data_{str(uuid.uuid4())}.pkl"
            local_filename = os.path.join(S3XComBackend.LOCAL_PREFIX, filename)
            s3_filename = S3XComBackend.S3_PREFIX + filename

            with open(local_filename, 'wb') as f:
                pickle.dump(value, f)
                print(f"Value '{value}' dump in {local_filename}")

            hook.load_file(filename=local_filename,
                           key=s3_filename,
                           bucket_name=S3XComBackend.BUCKET_NAME,
                           replace=True)
            print(f"{local_filename} was load to {S3XComBackend.BUCKET_NAME} bucket")

        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)

        if isinstance(result, str) and result.startswith(S3XComBackend.S3_PREFIX):
            hook = S3Hook()
            key = result
            filename = hook.download_file(key=key,
                                          bucket_name=S3XComBackend.BUCKET_NAME,
                                          local_path=S3XComBackend.S3_PREFIX)
            result = pickle.load(filename)
            print(f"{key} was download from {S3XComBackend.BUCKET_NAME} bucket")

        return result
