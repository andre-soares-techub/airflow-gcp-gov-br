from helpers.utils.dataproc.dataproc_config import (
    get_cluster_config
    )
from helpers.utils.general_config import PathsDataLake
from helpers.apis.dicts.api_gov_br.base import (PROJECT_ID, PYSPARK_FILE, DATASET_ID_INCOMING, DATASET_ID_RAW, 
                                                DATASET_ID_TRUSTED, LOCATION,SERVICE_ACCOUNT, GCP_CONN_ID,
                                                DEFAULT_RETRY)


# constantes com os nomes das tabela
TABLE='api-orgaos'

# ----------------------------------------------------------------------------

# configuração das pastas no datalake
path_dl = PathsDataLake(change_file_type='csv', change_table_name=TABLE, change_file_extension='csv', flow_technology='dataproc')

# ----------------------------------------------------------------------------

# tamanho do cluster de dataproc, temos 3 tamanhos e eles podem ser vistos no arquivo 
# airflow-gcp-gov-br/dags/helpers/utils/dataproc/dataproc_config.py
cluster_config = 'medium'

# ----------------------------------------------------------------------------


# constantes com endereços utilizados para a tabela api_orgaos
PATH_SAVE_FILE_TABLE_INCOMING=path_dl.change_file_path(change_layer=DATASET_ID_INCOMING)
PATH_SAVE_FILE_TABLE_RAW=path_dl.change_file_path(change_layer=DATASET_ID_RAW)
PATH_SAVE_FILE_TABLE_TRUSTED=path_dl.change_file_path(change_layer=DATASET_ID_TRUSTED)

# ----------------------------------------------------------------------------


# nomes de clusters 
CLUSTER_NAME_INCOMING = path_dl.get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_INCOMING)
CLUSTER_NAME_RAW = path_dl.get_cluster_name(project_id=PROJECT_ID, layer=DATASET_ID_RAW)

# ----------------------------------------------------------------------------

# constantes com URIs de cada api
URI_TABLE = 'http://compras.dados.gov.br/licitacoes/v1/orgaos.csv'

# ----------------------------------------------------------------------------

# quais colunas serão usadas para a clusterização da tabela no bigquery
CLUSTER_FIELDS = 'codigo_tipo_adm', 'codigo_tipo_poder'

# ----------------------------------------------------------------------------

TASK_CONFIG = {
    "ZONE":LOCATION,
    "TABLE_NAME":TABLE,
    "CLOUD_FUNCTION":{
        'task_id':'extration',
        'method':'GET',
        'http_conn_id':'http_conn_id_data_gov_br',
        'endpoint':'formiga-cortadeira',
        'data':{
            'URL_API':URI_TABLE,
            'PROJECT_ID':PROJECT_ID,
            'BUCKET_NAME':PROJECT_ID,
            'PATH_FILE':PATH_SAVE_FILE_TABLE_INCOMING
        },
        'headers':{"Content-Type": "application/json"}
    },
    'DELETE_FILE':{
        'INCOMING':{
            'bucket_name':PROJECT_ID,
            'prefix':PATH_SAVE_FILE_TABLE_INCOMING,
            'gcp_conn_id':GCP_CONN_ID,
            'impersonation_chain':SERVICE_ACCOUNT
        },
    },
    'DATAPROC_CONFIG':{
        'INCOMING_TO_RAW':{
            'CREATE_CLUSTER':{
                'task_id':f'incoming_create_cluster_{TABLE}',
                'project_id':PROJECT_ID,
                'cluster_config': get_cluster_config(cluster_config),
                'region':LOCATION,
                'gcp_conn_id':GCP_CONN_ID,
                'cluster_name':CLUSTER_NAME_INCOMING
            },
            'SUBMIT_JOB_SPARK':{
                'task_id':f'submit_job_spark_{TABLE}',
                'job':{
                    'reference': {'project_id':PROJECT_ID},
                    'placement': {'cluster_name': CLUSTER_NAME_INCOMING},
                    'pyspark_job':{
                        'main_python_file_uri': PYSPARK_FILE,
                        'args': [
                            f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_TABLE_INCOMING}',
                            f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_TABLE_RAW}',
                            f'incoming_to_raw-{TABLE}',
                            f'gs://{PROJECT_ID}/{path_dl.get_file_check_path(DATASET_ID_RAW)}'
                        ]
                        }
                },
                'region':LOCATION,
                'project_id':PROJECT_ID,
                'gcp_conn_id':GCP_CONN_ID
            },
            'DELETE_CLUSTER':{
                'task_id':f'incoming_delete_cluster_{TABLE}',
                'project_id':PROJECT_ID,
                'region':LOCATION,
                'cluster_name':CLUSTER_NAME_INCOMING,
                'gcp_conn_id':GCP_CONN_ID
            },
        },
        'RAW_TO_TRUSTED':{
            'CREATE_CLUSTER':{
                'task_id':f'raw_create_cluster_{TABLE}',
                'project_id':PROJECT_ID,
                'cluster_config': get_cluster_config(cluster_config),
                'region':LOCATION,
                'gcp_conn_id':GCP_CONN_ID,
                'cluster_name':CLUSTER_NAME_RAW
            },
            'SUBMIT_JOB_SPARK':{
                'task_id':f'submit_job_spark_{TABLE}',
                'job':{
                    'reference':{"project_id": PROJECT_ID},
                    'placement':{'cluster_name': CLUSTER_NAME_RAW},
                    'pyspark_job':{
                        'main_python_file_uri': PYSPARK_FILE,
                        'args':[
                            f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_TABLE_RAW}',
                            f'gs://{PROJECT_ID}/{PATH_SAVE_FILE_TABLE_TRUSTED}',
                            f'raw_to_trusted-{TABLE}',
                            ''
                        ]
                        },
                },
                'region':LOCATION,
                'project_id':PROJECT_ID,
                'gcp_conn_id':GCP_CONN_ID
            },
            'DELETE_CLUSTER':{
                'task_id':f'raw_delete_cluster_{TABLE}',
                'project_id':PROJECT_ID,
                'region':LOCATION,
                'cluster_name':CLUSTER_NAME_RAW,
                'gcp_conn_id':GCP_CONN_ID
            },
        },
    },
    'TRUSTED_TO_BIGQUERY':{
        'task_id':f'trusted_to_bigquery-{TABLE}',
        'bucket': PROJECT_ID,
        'source_objects':f'{PATH_SAVE_FILE_TABLE_TRUSTED}/part*',
        'destination_project_dataset_table':f'{PROJECT_ID}.api.{TABLE}',
        'source_format':'csv',
        'compression':'GZIP',
        'create_disposition':'CREATE_IF_NEEDED',
        'write_disposition':'WRITE_APPEND',
        'field_delimiter':',',
        'quote_character':'"',
        'ignore_unknown_values':True,
        'allow_jagged_rows':False,
        'encoding':'UTF-8',
        'gcp_conn_id':'gcp_conn_id',
        'time_partitioning': {
            'field':'ingestion_date',
            'type':'MONTH',
            'expiration_ms': None,
            'require_partition_filter': True
        },
        'cluster_fields': (CLUSTER_FIELDS),
        'autodetect':True,
        'location':LOCATION,
        'impersonation_chain':SERVICE_ACCOUNT,
        'result_retry':DEFAULT_RETRY,
        'result_timeout': None
    }               
} 