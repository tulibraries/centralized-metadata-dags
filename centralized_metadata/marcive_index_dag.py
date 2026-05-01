""" Airflow DAG to index Authority data"""
from datetime import timedelta

import pendulum
from airflow.sdk import DAG as AirflowDAG
from airflow.providers.standard.operators.bash import BashOperator

MARC_FILES_SFTP_CONNECTION_ID = "MARC_FILES_SFTP"
CENTRALIZED_METADATA_API_CONNECTION_ID = "CENTRALIZED_METADATA_API"
SCRIPT_PATH = "/opt/airflow/dags/repo/centralized_metadata/scripts/ftp-index-marc-records.sh "

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2018, 12, 13, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def _marcive_env(endpoint_path):
    """Build runtime-templated environment variables for the ingest script."""
    sftp_conn = f"conn.get('{MARC_FILES_SFTP_CONNECTION_ID}')"
    cm_api_conn = f"conn.get('{CENTRALIZED_METADATA_API_CONNECTION_ID}')"

    return {
        "FTP_SERVER": f"{{{{ {sftp_conn}.host }}}}",
        "FTP_PORT": f"{{{{ {sftp_conn}.port or 22 }}}}",
        "FTP_USER": f"{{{{ {sftp_conn}.login }}}}",
        "FTP_ID_PATH": f"{{{{ {sftp_conn}.extra_dejson.key_file }}}}",
        "CM_API_ENDPOINT": (
            f"{{{{ {cm_api_conn}.get_uri().rstrip('/') }}}}"
            f"{endpoint_path}"
        ),
    }

DAG = AirflowDAG(
    'marcive_ingest',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    #schedule=SCHEDULE
)

"""
CREATE TASKS

Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""


INDEX_WEB_CONTENT = BashOperator(
    task_id="get_and_ingest_marcive_records",
    bash_command=SCRIPT_PATH,
    env=_marcive_env("/records"),
    dag=DAG
)

DELETE_RECORDS = BashOperator(
    task_id="get_and_delete_marcive_records",
    bash_command=SCRIPT_PATH,
    env=_marcive_env("/marc_file/delete"),
    dag=DAG
)


DELETE_RECORDS.set_upstream(INDEX_WEB_CONTENT)
