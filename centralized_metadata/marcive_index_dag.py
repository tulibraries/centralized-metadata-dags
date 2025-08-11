""" Airflow DAG to index Web Content into SolrCloud. """
from datetime import timedelta
import airflow
import pendulum
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator

SFTP = BaseHook.get_connection("MARC_FILES_SFTP")
HTTP = BaseHook.get_connection("CENTRALIZED_METADATA_API")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2018, 12, 13, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    #'on_failure_callback': tasks.execute_slackpostonfail,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'marcive_ingest',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    #schedule=SCHEDULE_INTERVAL
)

"""
CREATE TASKS

Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""


INGEST = BashOperator(
    task_id="get_and_ingest_marcive_records",
    bash_command="/opt/airflow/dags/repo/centralized_metadata/scripts/ftp-index-marc-records.sh ",
    env={
        "FTP_SERVER": SFTP.host,
        "FTP_PORT": str(SFTP.port),
        "FTP_USER": SFTP.login,
        "FTP_ID_PATH": "/home/airflow/dspacesftp@ftp_prod-private-key",
        "CM_API_ENDPOINT": HTTP.get_uri() + "/records",
    },
    dag=DAG)
