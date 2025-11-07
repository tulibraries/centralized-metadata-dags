"""Airflow DAG to run OCRmyPDF across a target directory."""
import logging
import subprocess
from datetime import timedelta
from pathlib import Path

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators.python import PythonOperator

DEFAULT_SHARE_ROOT = "/opt/airflow/shared"
DEFAULT_RELATIVE_PATH = "DataPreservationStaging/OCRMyPDFs"
DEFAULT_PDF_DIRECTORY = str(Path(DEFAULT_SHARE_ROOT) / DEFAULT_RELATIVE_PATH)
SHARE_ROOT_VARIABLE = "OCR_PDF_SHARE_ROOT"
RELATIVE_PATH_VARIABLE = "OCR_PDF_RELATIVE_PATH"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _resolve_pdf_directory(context):
    """Resolve the directory Airflow should scan for PDFs."""
    params = (context or {}).get("params") or {}
    dag_run = (context or {}).get("dag_run")

    if dag_run and getattr(dag_run, "conf", None):
        conf_directory = dag_run.conf.get("pdf_directory")
        if conf_directory:
            return Path(conf_directory).expanduser().resolve()

    user_directory = params.get("pdf_directory")
    if user_directory:
        return Path(user_directory).expanduser().resolve()

    share_root = Path(
        Variable.get(SHARE_ROOT_VARIABLE, default_var=DEFAULT_SHARE_ROOT)
    ).expanduser()
    relative_path_value = Variable.get(
        RELATIVE_PATH_VARIABLE, default_var=DEFAULT_RELATIVE_PATH
    )
    relative_path = Path(relative_path_value)
    configured_directory = (
        relative_path if relative_path.is_absolute() else share_root / relative_path
    )
    return configured_directory.resolve()


def process_pdfs(**context):
    """Iterate through the directory and run OCRmyPDF with optimize level 1."""
    pdf_directory = _resolve_pdf_directory(context)
    if not pdf_directory.exists():
        raise FileNotFoundError(f"{pdf_directory} does not exist")

    pdf_files = sorted(
        pdf_path for pdf_path in pdf_directory.glob("*.pdf") if pdf_path.is_file()
    )

    if not pdf_files:
        logging.info("No PDF files found in %s", pdf_directory)
        return "no-pdfs-found"

    for pdf_file in pdf_files:
        output_pdf = pdf_file.with_name(f"{pdf_file.stem}_ocr.pdf")
        command = [
            "ocrmypdf",
            "--optimize",
            "1",
            str(pdf_file),
            str(output_pdf),
        ]
        logging.info("Running command: %s", " ".join(command))
        subprocess.run(command, check=True)  # Raises CalledProcessError on failure.

    return f"processed-{len(pdf_files)}-pdfs"


DAG = airflow.DAG(
    "ocrmypdf_batch",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,
)

RUN_OCR = PythonOperator(
    task_id="run_ocrmypdf",
    python_callable=process_pdfs,
    provide_context=True,
    params={"pdf_directory": DEFAULT_PDF_DIRECTORY},
    dag=DAG,
)
