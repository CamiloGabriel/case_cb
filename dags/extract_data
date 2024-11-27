from airflow.decorators import dag, task
from datetime import datetime
from google.cloud import storage
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv
import os

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from airflow.models.baseoperator import chain




BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_NAME = os.getenv("BIGQUERY_DATASET_NAME")




@dag(
    start_date=datetime(2024, 7, 7),
    schedule=None,
    catchup=False,
    tags=['erp', 'bi', 'restaurants'],
)
def erp_bi_data_extraction():
    BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
    DATASET_NAME=os.getenv("BIGQUERY_DATASET_NAME")
    BASE_URL=os.getenv("BASE_URL")
    API_KEY=os.getenv("API_KEY")
    gcs_path = os.getenv("GCS_PATH")
    @task
    def fetch_and_store_data(endpoint: str, store_id: str, bus_date: str):
        """
        Consome dados de uma API e armazena no formato Parquet no Google Cloud Storage.
        """
        base_url = BASE_URL
        headers = {"Authorization": "Bearer YOUR_API_KEY"}
        payload = {"busDt": bus_date, "storeId": store_id}

        response = requests.post(f"{base_url}{endpoint}", json=payload, headers=headers)
        response.raise_for_status()  
        data = response.json()
        df = pd.json_normalize(data)

        table = pa.Table.from_pandas(df)
        file_name = f"{endpoint.strip('/').replace('/', '_')}_{store_id}_{bus_date}.parquet"
        local_path = f"/tmp/{file_name}"
        pq.write_table(table, local_path)

        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path + file_name)
        blob.upload_from_filename(local_path)

        return f"Dados do endpoint {endpoint} para a loja {store_id} armazenados com sucesso."
    @task
    def process_all_endpoints(bus_date: str):
        """
        Processa todos os endpoints para todas as lojas.
        """
        endpoints = [
            "/bi/getFiscalInvoice",
            "/res/getGuestChecks",
            "/org/getChargeBack",
            "/trans/getTransactions",
            "/inv/getCashManagementDetails",
        ]
        store_ids = ["store_1", "store_2", "store_3"]

        results = []
        for endpoint in endpoints:
            for store_id in store_ids:
                results.append(fetch_and_store_data(endpoint, store_id, bus_date))
        return results

    operation_date = "2024-11-01"  

    process_all_endpoints(operation_date)


erp_bi_data_extraction_dag = erp_bi_data_extraction()