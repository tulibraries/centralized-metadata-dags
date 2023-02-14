"""Airflow DAG"""
from datetime import timedelta
import airflow
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
# AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
hello_message = Variable.get("hello_message")

# CREATE DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2018, 12, 13, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'hello_world',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule="@weekly"
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

SET_COLLECTION_NAME = BashOperator(
    task_id="set_collection_name",
    bash_command='echo ' + hello_message,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
