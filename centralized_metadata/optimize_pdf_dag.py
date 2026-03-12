"""Airflow DAG to run OCRmyPDF across a target directory."""
import json
import logging
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

DEFAULT_SHARE_ROOT = "/opt/airflow/shared"
DEFAULT_RELATIVE_PATH = "DataPreservationStaging/OCRMyPDFs"
DEFAULT_PDF_DIRECTORY = str(Path(DEFAULT_SHARE_ROOT) / DEFAULT_RELATIVE_PATH)
SHARE_ROOT_VARIABLE = "OCR_PDF_SHARE_ROOT"
RELATIVE_PATH_VARIABLE = "OCR_PDF_RELATIVE_PATH"
DEFAULT_SCHEDULE_INTERVAL = "@weekly"
SCHEDULE_INTERVAL_VARIABLE = "OPTIMIZE_PDF_SCHEDULE_INTERVAL"
TEAMS_WEBHOOK_VARIABLE = "OPTIMIZE_PDF_TEAMS_WEBHOOK_URL"

slackpostonfail = send_slack_notification(
    channel="infra_alerts",
    username="airflow",
    text=(
        ":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} "
        "{{ dag_run.logical_date }} {{ ti.log_url }}"
    ),
)


def _post_to_teams(title, text, theme_color):
    """Send a message card to Teams using the configured webhook."""
    webhook_url = Variable.get(TEAMS_WEBHOOK_VARIABLE, default_var=None)
    if not webhook_url:
        logging.warning(
            "Skipping Teams notification because %s is not set", TEAMS_WEBHOOK_VARIABLE
        )
        return

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
        "themeColor": theme_color,
        "title": title,
        "text": text,
    }
    request_obj = Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request_obj, timeout=10):
            return
    except URLError as exc:
        logging.error("Failed to send Teams notification: %s", exc)


def _format_context_lines(context):
    ti = context.get("ti") or context.get("task_instance")
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = (dag.dag_id if dag else None) or (
        dag_run.dag_id if dag_run else "unknown"
    )
    task_id = ti.task_id if ti else "unknown"
    run_id = dag_run.run_id if dag_run else ""
    logical_date = getattr(dag_run, "logical_date", None)
    log_url = getattr(ti, "log_url", "")
    lines = [
        f"**DAG**: {dag_id}",
        f"**Task**: {task_id}",
    ]
    if run_id:
        lines.append(f"**Run ID**: {run_id}")
    if logical_date:
        lines.append(f"**Logical Date**: {logical_date}")
    if log_url:
        lines.append(f"[View log]({log_url})")
    return dag_id, "\n".join(lines)


def teamspostonfail(context):
    """Send a Teams notification when a task fails."""
    dag_id, context_text = _format_context_lines(context)
    _post_to_teams(
        f"DAG {dag_id} task failure",
        context_text,
        theme_color="C4463D",
    )


def teamspostonsuccess(context):
    """Send a Teams notification when the workflow succeeds."""
    dag_id, context_text = _format_context_lines(context)
    _post_to_teams(
        f"DAG {dag_id} completed successfully",
        context_text,
        theme_color="2EB886",
    )



DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": [slackpostonfail,teamspostonfail],
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


def run_and_stream(command, prefix=None):
    """
    Run a subprocess command, stream output to logs in real time,
    and raise CalledProcessError if the command fails.
    """
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as process:
        if process.stdout is None:
            raise RuntimeError("stdout pipe was not created")

        for line in process.stdout:
            line = line.rstrip()
            if prefix:
                logging.info("[%s] %s", prefix, line)
            else:
                logging.info("%s", line)

        return_code = process.wait()

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


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

        if output_pdf.exists():
            logging.info("Removing existing optimized PDF: %s", output_pdf)
            output_pdf.unlink()

        command = [
            "ocrmypdf",
            "--skip-text",
            "--optimize",
            "1",
            str(pdf_file),
            str(output_pdf),
        ]

        logging.info("Running command: %s", " ".join(command))

        run_and_stream(command, prefix=pdf_file.name)

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
    schedule_interval=Variable.get(
        SCHEDULE_INTERVAL_VARIABLE, default_var=DEFAULT_SCHEDULE_INTERVAL
    ),
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

SUCCESS = EmptyOperator(
    task_id='success',
    on_success_callback=[teamspostonsuccess],
    trigger_rule="none_failed_min_one_success",
    dag=DAG,
)

RUN_OCR.set_downstream(MOVE_PROCESSED_FILES)
MOVE_PROCESSED_FILES.set_downstream(SUCCESS)
