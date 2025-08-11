"""Unit Tests for the DAG."""
import os
import unittest
import airflow
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
            ])
