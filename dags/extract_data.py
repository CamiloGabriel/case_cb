from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator
)
from datetime import datetime
from google.cloud import storage
import requests
import pandas as pd
import os
from dotenv import load_dotenv
import redis
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_NAME = os.getenv("BIGQUERY_DATASET_NAME")
MEMORSTORE_HOST = os.getenv("MEMORSTORE_HOST")
MEMORSTORE_PORT = int(os.getenv("MEMORSTORE_PORT"))
MEMORSTORE_PASSWORD = os.getenv("MEMORSTORE_PASSWORD")

redis_client = redis.StrictRedis(
    host=MEMORSTORE_HOST,
    port=MEMORSTORE_PORT,
    password=MEMORSTORE_PASSWORD,
    decode_responses=True
)

@dag(
    start_date=datetime(2024, 7, 7),
    schedule=None,
    catchup=False,
    tags=['data-lake', 'bigquery', 'etl'],
)
def erp_bi_data_extraction_with_parquet():
    """
    Pipeline de extração de dados de APIs e do arquivo ERP.json,
    com armazenamento no GCS (formato Parquet) e carregamento no BigQuery.
    """
    @task
    def process_local_guest_checks(file_path: str, bus_date: str, store_id: str):
        """
        Processa o arquivo ERP.JSON local de guest checks e converte para Parquet.
        """
        print(f"Lendo o arquivo local: {file_path}")
        data = pd.read_json(file_path)

        data_type = "guest_checks"
        category = "api"
        local_parquet_file = f"/tmp/{data_type}_store_{store_id}_{bus_date}.parquet"

        print(f"Convertendo dados para Parquet...")
        table = pa.Table.from_pandas(data)
        pq.write_table(table, local_parquet_file)

        return {
            "local_file": local_parquet_file,
            "data_type": data_type,
            "category": category,
            "store_id": store_id,
            "bus_date": bus_date,
        }

    @task
    def upload_parquet_to_gcs(local_data):
        """
        Faz o upload de um arquivo Parquet local para o Google Cloud Storage.
        """
        gcs_path = f"data-lake/raw/{local_data['category']}/{local_data['data_type']}/{local_data['bus_date'][:4]}/{local_data['bus_date'][5:7]}/store_{local_data['store_id']}/{local_data['data_type']}_store_{local_data['store_id']}_{local_data['bus_date']}.parquet"
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        with open(local_data['local_file'], 'rb') as f:
            blob.upload_from_file(f)

        print(f"Arquivo enviado para o GCS: {gcs_path}")
        return {
            "gcs_path": gcs_path,
            "data_type": local_data["data_type"],
            "store_id": local_data["store_id"],
            "bus_date": local_data["bus_date"],
        }

    @task
    def fetch_and_store_data(endpoint: str, store_id: str, bus_date: str, data_type: str, category: str):
        """
        Consome dados da API e armazena no Google Cloud Storage em formato Parquet.
        """
        cache_key = f"{endpoint.strip('/')}_{store_id}_{bus_date}"
        cached_data = redis_client.get(cache_key)

        if cached_data:
            print(f"Cache hit for {cache_key}")
            data = pd.read_json(cached_data)
        else:
            print(f"Cache miss for {cache_key}, fetching from API...")
            headers = {"Authorization": f"Bearer {API_KEY}"}
            payload = {"busDt": bus_date, "storeId": store_id}

            response = requests.post(f"{BASE_URL}{endpoint}", json=payload, headers=headers)
            response.raise_for_status()
            data = pd.json_normalize(response.json())

            redis_client.setex(cache_key, 3600, data.to_json())

        gcs_path = f"data-lake/raw/{category}/{data_type}/{bus_date[:4]}/{bus_date[5:7]}/store_{store_id}/{data_type}_store_{store_id}_{bus_date}.parquet"

        table = pa.Table.from_pandas(data)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)

        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(buffer.getvalue().to_pybytes(), content_type="application/octet-stream")

        return {
            "gcs_path": gcs_path,
            "data_type": data_type,
            "bus_date": bus_date,
            "store_id": store_id,
        }

    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset",
        dataset_id='retail',
        gcp_conn_id='gcp',
        exists_ok=True,
    )

    @task
    def process_all_endpoints(bus_date: str):
        """
        Processa todos os endpoints para todas as lojas.
        """
        endpoints = {
            "/bi/getFiscalInvoice": ("fiscal_invoices", "api"),
            "/res/getGuestChecks": ("guest_checks", "api"),
            "/org/getChargeBack": ("chargebacks", "api"),
            "/trans/getTransactions": ("transactions", "api"),
            "/inv/getCashManagementDetails": ("cash_management", "api"),
        }
        store_ids = ["001", "002", "003"]

        results = []
        for endpoint, (data_type, category) in endpoints.items():
            for store_id in store_ids:
                result = fetch_and_store_data(endpoint, store_id, bus_date, data_type, category)
                results.append(result)
        return results

    @task
    def upload_to_bigquery(fetched_data, uploaded_local_data):
        """
        Carrega os dados do GCS para o BigQuery.
        """
        results = [*fetched_data, uploaded_local_data]  
        for result in results:
            BigQueryInsertJobOperator(
                task_id=f"load_{result['data_type']}_{result['store_id']}",
                gcp_conn_id='gcp',
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{BUCKET_NAME}/{result['gcs_path']}"],
                        "destinationTable": {
                            "projectId": "{{ var.value.gcp_project_id }}",
                            "datasetId": DATASET_NAME,
                            "tableId": f"{result['data_type']}_store_{result['store_id']}"
                        },
                        "sourceFormat": "PARQUET",
                        "writeDisposition": "WRITE_APPEND",
                    }
                },
            )


    operation_date = "2024-05-05"
    local_file_path = "/usr/local/airflow/docs/ERP.json"
    store_id = "001"

#     fetched_data = process_all_endpoints(operation_date)
#     local_data = process_local_guest_checks(local_file_path, operation_date, store_id)
#     uploaded_local_data = upload_parquet_to_gcs(local_data)

#     upload_to_bigquery(fetched_data, uploaded_local_data) >> create_bigquery_dataset

# Esse é um fluxo de execução das tasks onde uma task depende da outra, nesse caso, o arquivo JSON que foi transformado em Parquet só será carregado no Bigquery após que extrai dados dos endpoints ser concluída com sucesso. Dessa forma, é possível obter uma maior escalabilidade, isolmento de falhas, etc. Porém, para verificar se a pipeline está funcional, será usado o fluxo abaixo:


    local_data = process_local_guest_checks(local_file_path, operation_date, store_id)
    uploaded_local_data = upload_parquet_to_gcs(local_data)

    
    upload_to_bigquery([uploaded_local_data], None)  

    
    fetched_data = process_all_endpoints(operation_date)
    upload_to_bigquery(fetched_data, None)  

    create_bigquery_dataset


erp_bi_data_extraction_with_parquet_dag = erp_bi_data_extraction_with_parquet()
