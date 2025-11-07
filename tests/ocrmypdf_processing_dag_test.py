"""Unit tests for the OCRmyPDF DAG."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from centralized_metadata.ocrmypdf_processing_dag import DAG, process_pdfs


class TestOCRMyPDFDag(unittest.TestCase):
    """Basic regression coverage for the OCRmyPDF DAG."""

    def test_dag_loads(self):
        """Ensure the DAG is registered with the expected ID and task."""
        self.assertEqual(DAG.dag_id, "ocrmypdf_batch")
        task_ids = [task.task_id for task in DAG.tasks]
        self.assertEqual(task_ids, ["run_ocrmypdf"])

    @mock.patch("centralized_metadata.ocrmypdf_processing_dag.subprocess.run")
    def test_process_pdfs_runs_command_per_file(self, mock_run):
        """Touch PDFs and verify we issue an OCR command for each."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_one = os.path.join(tmp_dir, "file_one.pdf")
            pdf_two = os.path.join(tmp_dir, "file_two.pdf")
            open(pdf_one, "wb").close()
            open(pdf_two, "wb").close()

            result = process_pdfs(params={"pdf_directory": tmp_dir})

            self.assertEqual(result, "processed-2-pdfs")
            self.assertEqual(mock_run.call_count, 2)

            expected_files = [Path(pdf_one), Path(pdf_two)]
            for call_args, expected_file in zip(mock_run.call_args_list, expected_files):
                command = call_args.args[0]
                self.assertEqual(command[:3], ["ocrmypdf", "--optimize", "1"])
                self.assertEqual(Path(command[3]).name, expected_file.name)
                self.assertEqual(
                    Path(command[4]).name, f"{expected_file.stem}_ocr.pdf"
                )
                self.assertTrue(call_args.kwargs.get("check"))

    @mock.patch("centralized_metadata.ocrmypdf_processing_dag.subprocess.run")
    def test_process_pdfs_handles_empty_directory(self, mock_run):
        """Confirm we short-circuit gracefully when no PDFs exist."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = process_pdfs(params={"pdf_directory": tmp_dir})

            self.assertEqual(result, "no-pdfs-found")
            mock_run.assert_not_called()

    @mock.patch("centralized_metadata.ocrmypdf_processing_dag.subprocess.run")
    def test_process_pdfs_prefers_dag_run_conf(self, mock_run):
        """dag_run.conf should override params and defaults."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_path = os.path.join(tmp_dir, "file.pdf")
            open(pdf_path, "wb").close()

            dag_run_mock = mock.Mock()
            dag_run_mock.conf = {"pdf_directory": tmp_dir}

            result = process_pdfs(dag_run=dag_run_mock, params={"pdf_directory": "/unused"})

            self.assertEqual(result, "processed-1-pdfs")
            mock_run.assert_called_once()

    @mock.patch("centralized_metadata.ocrmypdf_processing_dag.Variable.get")
    @mock.patch("centralized_metadata.ocrmypdf_processing_dag.subprocess.run")
    def test_process_pdfs_uses_share_root_and_relative_path(self, mock_run, mock_variable_get):
        """Variable-based root + relative path should resolve to final directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            share_root = tmp_dir
            relative_path = "incoming"
            target_dir = Path(share_root) / relative_path
            target_dir.mkdir()
            pdf_path = target_dir / "variable.pdf"
            pdf_path.touch()

            def fake_variable_get(key, default_var=None):
                if key == "OCR_PDF_SHARE_ROOT":
                    return share_root
                if key == "OCR_PDF_RELATIVE_PATH":
                    return relative_path
                return default_var

            mock_variable_get.side_effect = fake_variable_get

            result = process_pdfs()

            self.assertEqual(result, "processed-1-pdfs")
            command = mock_run.call_args.args[0]
            self.assertEqual(Path(command[3]).resolve(), pdf_path.resolve())

if __name__ == "__main__":
    unittest.main()
