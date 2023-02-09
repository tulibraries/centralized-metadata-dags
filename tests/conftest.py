# PyTest Configuration file.
import os
import subprocess
from airflow.models import Variable, Connection
from airflow.settings import Session

def pytest_sessionstart():
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    repo_dir = os.getcwd()
    subprocess.run("airflow db init", shell=True)
    subprocess.run("mkdir -p dags/centralized_metadata", shell=True)
    subprocess.run("mkdir -p data", shell=True)
    subprocess.run("mkdir -p logs", shell=True)
    subprocess.run("cp ./centralized_metadata/*.py dags/centralized_metadata", shell=True)

    airflow_session = Session()
    airflow_session.commit()

def pytest_sessionfinish():
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    subprocess.run("rm -rf dags", shell=True)
    subprocess.run("rm -rf data", shell=True)
    subprocess.run("rm -rf logs", shell=True)
    subprocess.run("yes | airflow db reset", shell=True)
