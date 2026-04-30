"""Pytest configuration for Airflow DAG tests."""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="centralized-metadata-airflow-"))
AIRFLOW_BIN = Path(sys.executable).with_name("airflow")
DAGS_FOLDER = AIRFLOW_HOME / "dags"
LOGS_FOLDER = AIRFLOW_HOME / "logs"
DATA_FOLDER = AIRFLOW_HOME / "data"
AIRFLOW_DB = AIRFLOW_HOME / "airflow.db"

os.environ["AIRFLOW_HOME"] = str(AIRFLOW_HOME)
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS_FOLDER)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{AIRFLOW_DB}"
os.environ["AIRFLOW__LOGGING__BASE_LOG_FOLDER"] = str(LOGS_FOLDER)
os.environ["AIRFLOW_CONN_CENTRALIZED_METADATA_API"] = "http://127.0.0.1"
os.environ["AIRFLOW_CONN_MARC_FILES_SFTP"] = "sftp://airflow@127.0.0.1:22"
os.environ["AIRFLOW_VAR_HELLO_MESSAGE"] = "hola"
os.environ["AIRFLOW_VAR_OPTIMIZE_PDF_SCHEDULE"] = "@weekly"


def pytest_sessionstart(session):
    """Create an isolated Airflow home and migrate the metadata DB."""
    del session
    dag_target = DAGS_FOLDER / "centralized_metadata"
    dag_target.mkdir(parents=True, exist_ok=True)
    LOGS_FOLDER.mkdir(parents=True, exist_ok=True)
    DATA_FOLDER.mkdir(parents=True, exist_ok=True)

    for dag_file in (REPO_ROOT / "centralized_metadata").glob("*.py"):
        shutil.copy2(dag_file, dag_target / dag_file.name)

    airflow_command = str(AIRFLOW_BIN) if AIRFLOW_BIN.exists() else "airflow"
    subprocess.run([airflow_command, "db", "migrate"], check=True)


def pytest_sessionfinish(session, exitstatus):
    """Remove the isolated Airflow home after tests finish."""
    del session, exitstatus
    shutil.rmtree(AIRFLOW_HOME, ignore_errors=True)
