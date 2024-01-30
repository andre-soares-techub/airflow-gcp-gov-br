from datetime import datetime, timedelta
from airflow.models import Variable

# constantes de uso geral

DATASET_ID_INCOMING="incoming"
DATASET_ID_RAW="raw"
DATASET_ID_TRUSTED="trusted"
LOCATION="us-central1"
SERVICE_ACCOUNT=Variable.get('SERVICE_ACCOUNT')
GCP_CONN_ID= 'gcp_conn_id'
DEFAULT_RETRY=4

PROJECT_ID="tutoriais-ed"
PYSPARK_FILE=f'gs://{PROJECT_ID}/script_submit_spark/api-gcp-gov-br/submit_file_config.py'


CALL_API_GOV = {
    "DAG_CONFIG":{
        "DAG_ID":"apis_gov_br_batch_dataproc",
        "PROJECT_ID":PROJECT_ID,
        "DEFAULT_ARGS":{
            'owner':'Alison',
            'start_date':datetime(2023, 7, 12),
            'end_date':datetime(2023, 7, 13),
            'retries': 4,
            'retry_delay': timedelta(seconds=120),
            'wait_for_downstream': True,
            'depends_on_past': True, 
        },
        'SCHEDULE_INTERVAL':'00 08 1-31 1-12 *',
        'CATCHUP':True, 
        'TAGS':['api_gov', 'cloud-function', 'dataproc', 'bigquery']
    },
}