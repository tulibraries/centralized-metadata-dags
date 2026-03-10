"""Airflow DAG to run OCRmyPDF across a target directory."""
import logging
import shutil
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
        return []

    processed_files = []
    for pdf_file in pdf_files:
        output_pdf = pdf_file.with_name(f"{pdf_file.stem}_opti.pdf")
        command = [
            "ocrmypdf",
            "--optimize",
            "1",
            str(pdf_file),
            str(output_pdf),
        ]
        logging.info("Running command: %s", " ".join(command))
        subprocess.run(command, check=True)  # Raises CalledProcessError on failure.
        processed_files.append(
            {"original": str(pdf_file), "optimized": str(output_pdf)}
        )

    return processed_files


def move_processed_pdfs(**context):
    """Move optimized and original PDFs into their destination directories."""
    ti = context["ti"]
    processed_files = ti.xcom_pull(task_ids="run_ocrmypdf") or []
    if not processed_files:
        logging.info("No processed files to move")
        return "no-files-to-move"

    moved_count = 0
    for file_info in processed_files:
        original_pdf = Path(file_info["original"])
        optimized_pdf = Path(file_info["optimized"])
        pdf_directory = original_pdf.parent
        optimized_directory = pdf_directory / "Optimized"
        originals_directory = pdf_directory / "Originals"
        optimized_directory.mkdir(parents=True, exist_ok=True)
        originals_directory.mkdir(parents=True, exist_ok=True)

        optimized_destination = optimized_directory / optimized_pdf.name
        originals_destination = originals_directory / original_pdf.name
        logging.info(
            "Moving %s to %s and %s to %s",
            optimized_pdf,
            optimized_destination,
            original_pdf,
            originals_destination,
        )
        shutil.move(str(optimized_pdf), str(optimized_destination))
        shutil.move(str(original_pdf), str(originals_destination))
        moved_count += 1

    return f"moved-{moved_count}-pdfs"


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

MOVE_PROCESSED_FILES = PythonOperator(
    task_id="move_processed_pdfs",
    python_callable=move_processed_pdfs,
    provide_context=True,
    dag=DAG,
)

RUN_OCR >> MOVE_PROCESSED_FILES
