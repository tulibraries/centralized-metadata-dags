"""Unit tests for the OCRmyPDF DAG."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from centralized_metadata.optimize_pdf_dag import DAG, process_pdfs


class TestOCRMyPDFDag(unittest.TestCase):
    """Basic regression coverage for the OCRmyPDF DAG."""

    def test_dag_loads(self):
        """Ensure the DAG is registered with the expected ID and task."""
        self.assertEqual(DAG.dag_id, "ocrmypdf_batch")
        task_ids = [task.task_id for task in DAG.tasks]
        self.assertEqual(task_ids, ["run_ocrmypdf", "success"])

    @mock.patch("centralized_metadata.optimize_pdf_dag.run_and_stream")
    def test_process_pdfs_runs_command_per_file(self, mock_run_and_stream):
        """Touch PDFs and verify we issue an OCR command for each."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_one = os.path.realpath(os.path.join(tmp_dir, "file_one.pdf"))
            pdf_two = os.path.realpath(os.path.join(tmp_dir, "file_two.pdf"))
            open(pdf_one, "wb").close()
            open(pdf_two, "wb").close()

            def fake_run(command, prefix=None):
                Path(command[5]).write_bytes(b"optimized")

            mock_run_and_stream.side_effect = fake_run

            result = process_pdfs(params={"pdf_directory": tmp_dir})

            self.assertEqual(
                result,
                [
                    {
                        "original": os.path.realpath(
                            os.path.join(tmp_dir, "Originals", "file_one.pdf")
                        ),
                        "optimized": os.path.realpath(
                            os.path.join(tmp_dir, "Optimized", "file_one_opti.pdf")
                        ),
                    },
                    {
                        "original": os.path.realpath(
                            os.path.join(tmp_dir, "Originals", "file_two.pdf")
                        ),
                        "optimized": os.path.realpath(
                            os.path.join(tmp_dir, "Optimized", "file_two_opti.pdf")
                        ),
                    },
                ],
            )
            self.assertEqual(mock_run_and_stream.call_count, 2)

            expected_files = [Path(pdf_one), Path(pdf_two)]
            for call_args, expected_file in zip(
                mock_run_and_stream.call_args_list, expected_files
            ):
                command = call_args.args[0]
                self.assertEqual(
                    command[:4], ["ocrmypdf", "--skip-text", "--optimize", "1"]
                )
                self.assertEqual(Path(command[4]).name, expected_file.name)
                self.assertEqual(
                    Path(command[5]).name, f"{expected_file.stem}_opti.pdf"
                )
                self.assertEqual(call_args.kwargs.get("prefix"), expected_file.name)

            originals_dir = Path(tmp_dir) / "Originals"
            optimized_dir = Path(tmp_dir) / "Optimized"
            self.assertTrue((originals_dir / "file_one.pdf").exists())
            self.assertTrue((originals_dir / "file_two.pdf").exists())
            self.assertTrue((optimized_dir / "file_one_opti.pdf").exists())
            self.assertTrue((optimized_dir / "file_two_opti.pdf").exists())

    @mock.patch("centralized_metadata.optimize_pdf_dag.run_and_stream")
    def test_process_pdfs_handles_empty_directory(self, mock_run_and_stream):
        """Confirm we short-circuit gracefully when no PDFs exist."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = process_pdfs(params={"pdf_directory": tmp_dir})

            self.assertEqual(result, [])
            mock_run_and_stream.assert_not_called()

    @mock.patch("centralized_metadata.optimize_pdf_dag.run_and_stream")
    def test_process_pdfs_prefers_dag_run_conf(self, mock_run_and_stream):
        """dag_run.conf should override params and defaults."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_path = os.path.realpath(os.path.join(tmp_dir, "file.pdf"))
            open(pdf_path, "wb").close()

            def fake_run(command, prefix=None):
                Path(command[5]).write_text("optimized")

            mock_run_and_stream.side_effect = fake_run

            dag_run_mock = mock.Mock()
            dag_run_mock.conf = {"pdf_directory": tmp_dir}

            result = process_pdfs(
                dag_run=dag_run_mock, params={"pdf_directory": "/unused"}
            )

            self.assertEqual(
                result,
                [
                    {
                        "original": os.path.realpath(
                            os.path.join(tmp_dir, "Originals", "file.pdf")
                        ),
                        "optimized": os.path.realpath(
                            os.path.join(tmp_dir, "Optimized", "file_opti.pdf")
                        ),
                    }
                ],
            )
            mock_run_and_stream.assert_called_once()

    @mock.patch("centralized_metadata.optimize_pdf_dag.Variable.get")
    @mock.patch("centralized_metadata.optimize_pdf_dag.run_and_stream")
    def test_process_pdfs_uses_share_root_and_relative_path(
        self, mock_run_and_stream, mock_variable_get
    ):
        """Variable-based root + relative path should resolve to final directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            share_root = os.path.realpath(tmp_dir)
            relative_path = "incoming"
            target_dir = Path(share_root) / relative_path
            target_dir.mkdir()
            pdf_path = target_dir / "variable.pdf"
            pdf_path.touch()

            def fake_run(command, prefix=None):
                Path(command[5]).write_bytes(b"optimized")

            mock_run_and_stream.side_effect = fake_run

            def fake_variable_get(key, default_var=None):
                if key == "OCR_PDF_SHARE_ROOT":
                    return share_root
                if key == "OCR_PDF_RELATIVE_PATH":
                    return relative_path
                return default_var

            mock_variable_get.side_effect = fake_variable_get

            result = process_pdfs()

            self.assertEqual(
                result,
                [
                    {
                        "original": str((target_dir / "Originals" / "variable.pdf").resolve()),
                        "optimized": str((target_dir / "Optimized" / "variable_opti.pdf").resolve()),
                    }
                ],
            )
            command = mock_run_and_stream.call_args.args[0]
            self.assertEqual(Path(command[4]).resolve(), pdf_path.resolve())

if __name__ == "__main__":
    unittest.main()
