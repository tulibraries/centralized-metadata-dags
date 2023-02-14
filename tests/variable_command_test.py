import os
import unittest
from airflow.models import Variable

class TestVariableCommand(unittest.TestCase):
    """Primary Class for Testing"""

    def test_variable_get(self):
        os.environ["AIRFLOW_VAR_FOO"] = "bar"
        self.assertEqual(Variable.get("foo"), "bar")
