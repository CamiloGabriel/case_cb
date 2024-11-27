from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
import json


from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from airflow.models.baseoperator import chain

load_dotenv()

BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_NAME = os.getenv("BIGQUERY_DATASET_NAME")


@dag(
    start_date=datetime(2024, 7, 7),
    schedule=None,
    catchup=False,
    tags=['data-lake', 'bigquery', 'etl'],
)
def erp_json_retail():
    """
    Pipeline de extração de dados do arquivo ERP.json, tratamento de erros, armazenamento no GCS (formato Parquet), controle de qualidade dos dados e criação de tabelas em SQL.
    """
    file_path = "/usr/local/airflow/docs/ERP.json"
    @task
    def process_erp_json_to_parquet(FILE_PATH: str):
        """
        Processa o arquivo ERP.JSON local, extrai 'curUTC' e 'locRef' do JSON, e converte para Parquet.
        """
        print(f"Lendo o arquivo local: {FILE_PATH}")

        with open(FILE_PATH, 'r') as f:
            json_data = json.load(f)

        curUTC = json_data["curUTC"][:10]  
        locRef = json_data["locRef"]

        curUTC_dt = datetime.strptime(curUTC, "%Y-%m-%d")
        year = curUTC_dt.year
        month = curUTC_dt.month
        day = curUTC_dt.day

        guest_checks = json_data["guestChecks"]

        processed_data = []
        for guest_check in guest_checks:
            for detail in guest_check["detailLines"]:
                menu_item = detail.get("menuItem", {})

                discount = menu_item.get("discount", "Not Provided")
                service_charge = menu_item.get("serviceCharge", "Not Provided")
                tender_media = menu_item.get("tenderMedia", "Not Provided")
                error_code = menu_item.get("errorCode", "Not Provided")

                taxes = None
                for key, value in guest_check.items():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        if all(k in value[0] for k in ["taxNum", "txblSlsTtl", "taxCollTtl"]):
                            taxes = value
                            break

                taxes = taxes or [{"taxNum": "Not Provided", "txblSlsTtl": 0, "taxCollTtl": 0}]

                for tax in taxes:
                    processed_data.append({
                        "curUTC": curUTC,
                        "locRef": locRef,
                        "guestCheckId": guest_check["guestCheckId"],
                        "chkNum": guest_check["chkNum"],
                        "opnBusDt": guest_check["opnBusDt"],
                        "opnUTC": guest_check["opnUTC"],
                        "opnLcl": guest_check["opnLcl"],
                        "clsdBusDt": guest_check["clsdBusDt"],
                        "clsdUTC": guest_check["clsdUTC"],
                        "clsdLcl": guest_check["clsdLcl"],
                        "lastTransUTC": guest_check["lastTransUTC"],
                        "lastTransLcl": guest_check["lastTransLcl"],
                        "lastUpdatedUTC": guest_check["lastUpdatedUTC"],
                        "lastUpdatedLcl": guest_check["lastUpdatedLcl"],
                        "clsdFlag": guest_check["clsdFlag"],
                        "gstCnt": guest_check["gstCnt"],
                        "subTtl": guest_check["subTtl"],
                        "nonTxblSlsTtl": guest_check["nonTxblSlsTtl"],
                        "chkTtl": guest_check["chkTtl"],
                        "dscTtl": guest_check["dscTtl"],
                        "payTtl": guest_check["payTtl"],
                        "balDueTtl": guest_check["balDueTtl"],
                        "rvcNum": guest_check["rvcNum"],
                        "otNum": guest_check["otNum"],
                        "ocNum": guest_check["ocNum"],
                        "tblNum": guest_check["tblNum"],
                        "tblName": guest_check["tblName"],
                        "empNum": guest_check["empNum"],
                        "numSrvcRd": guest_check["numSrvcRd"],
                        "numChkPrntd": guest_check["numChkPrntd"],
                        "taxNum": tax.get("taxNum", "Not Provided"),
                        "txblSlsTtl": tax.get("txblSlsTtl", 0),
                        "taxCollTtl": tax.get("taxCollTtl", 0),
                        "taxRate": tax.get("taxRate", 0),
                        "type": tax.get("type", 0),
                        "guestCheckLineItemId": detail["guestCheckLineItemId"],
                        "rvcNum": detail["rvcNum"],
                        "dtlOtNum": detail["dtlOtNum"],
                        "dtlOcNum": detail["dtlOcNum"],
                        "lineNum": detail["lineNum"],
                        "dtlId": detail["dtlId"],
                        "detailUTC": detail["detailUTC"],
                        "detailLcl": detail["detailLcl"],
                        "lastUpdateUTC": detail["lastUpdateUTC"],
                        "lastUpdateLcl": detail["lastUpdateLcl"],
                        "busDt": detail["busDt"],
                        "wsNum": detail["wsNum"],
                        "dspTtl": detail["dspTtl"],
                        "dspQty": detail["dspQty"],
                        "aggTtl": detail["aggTtl"],
                        "aggQty": detail["aggQty"],
                        "chkEmpId": detail["chkEmpId"],
                        "chkEmpNum": detail["chkEmpNum"],
                        "svcRndNum": detail["svcRndNum"],
                        "seatNum": detail["seatNum"],
                        "miNum": menu_item.get("miNum"),
                        "modFlag": menu_item.get("modFlag"),
                        "inclTax": menu_item.get("inclTax"),
                        "activeTaxes": menu_item.get("activeTaxes"),
                        "prcLvl": menu_item.get("prcLvl"),
                        "discount": discount,
                        "serviceCharge": service_charge,
                        "tenderMedia": tender_media,
                        "errorCode": error_code,
                    })

        data = pd.DataFrame(processed_data)

        local_parquet_file = f"/tmp/ERP_{locRef}_{curUTC}.parquet"

        print(f"Convertendo dados para Parquet...")
        table = pa.Table.from_pandas(data)
        pq.write_table(table, local_parquet_file)

        print(f"Arquivo Parquet gerado em: {local_parquet_file}")
        return {
            "local_file": local_parquet_file,
            "locRef": locRef,
            "curUTC": curUTC,
            "year": year,
            "month": month,
            "day": day,
        }


    parquet_data = process_erp_json_to_parquet(FILE_PATH=file_path)

    upload_parquet_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src="{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['local_file'] }}",
        dst=(
            "data-lake/raw/erp/store_{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['locRef'] }}/"
            "guest_checks/{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['year'] }}/"
            "{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['month'] }}/"
            "guest_checks_store_{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['locRef'] }}_"
            "{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['curUTC'] }}.parquet"
        ),
        bucket=BUCKET_NAME,
        gcp_conn_id='gcp',
        mime_type='application/octet-stream',
    )

    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset",
        dataset_id='retail',
        gcp_conn_id='gcp',
        exists_ok=True,
    )

    parquet_gcs_to_raw = aql.load_file(
        task_id='parquet_gcs_to_raw',
        input_file=File(
            path=(
                f"gs://{BUCKET_NAME}/data-lake/raw/erp/store_{{{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['locRef'] }}}}/"
                f"guest_checks/{{{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['year'] }}}}/"
                f"{{{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['month'] }}}}/"
                f"guest_checks_store_{{{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['locRef'] }}}}_"
                f"{{{{ ti.xcom_pull(task_ids='process_erp_json_to_parquet')['curUTC'] }}}}.parquet"
            ),
            conn_id='gcp',
            filetype=FileType.PARQUET,
        ),
        output_table=Table(
            name='raw_guest_checks',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=True,
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )


    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    

    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )


    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)


    chain(
        parquet_data,
        upload_parquet_to_gcs,
        create_bigquery_dataset,
        parquet_gcs_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report()
        )




erp_json_retail()