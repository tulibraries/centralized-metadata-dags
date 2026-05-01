"""Unit Tests for the DAG."""
import unittest

from centralized_metadata.marcive_index_dag import DAG


class TestMarciveIndexDag(unittest.TestCase):
    """Primary Class for Testing"""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "marcive_ingest")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "get_and_ingest_marcive_records",
            "get_and_delete_marcive_records",
            ])

    def test_task_env_uses_airflow_connection_templates(self):
        """Connection values should be resolved by Airflow at task runtime."""
        task = DAG.get_task("get_and_ingest_marcive_records")

        self.assertEqual(task.env["FTP_SERVER"], "{{ conn.MARC_FILES_SFTP.host }}")
        self.assertEqual(task.env["FTP_PORT"], "{{ conn.MARC_FILES_SFTP.port or 22 }}")
        self.assertEqual(task.env["FTP_USER"], "{{ conn.MARC_FILES_SFTP.login }}")
        self.assertEqual(
            task.env["CM_API_ENDPOINT"],
            "{{ conn.CENTRALIZED_METADATA_API.get_uri().rstrip('/') }}/records",
        )
