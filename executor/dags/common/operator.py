import os
import sys

import pytest
from airflow.operators.python import PythonOperator


def __run_pytest(test: str):
    path = os.path.join("/tests", test)
    sys.exit(pytest.main(["-x", path]))


def get_pytest_operator(task_id: str, test: str, **kwargs) -> PythonOperator:
    return PythonOperator(
        python_callable=__run_pytest,
        op_kwargs={"test": test},
        task_id=task_id,
        **kwargs
    )
