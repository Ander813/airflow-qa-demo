from datetime import timedelta, datetime
from typing import Any

from airflow import DAG
from airflow.models import BaseOperator

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from common.operator import get_pytest_operator

default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "test_voices",
    default_args=default_args,
    start_date=datetime.strptime("2025-06-1", "%Y-%m-%d"),
    schedule="0 4 * * *",
    catchup=False,
) as dag:
    run_smoke_test = get_pytest_operator(
        "insane_smoke_test", "test_insanity.py::test_voices"
    )
    run_test = get_pytest_operator(
        "sane_test",
        "test_insanity.py::test_creatures_in_the_walls",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    slack_notification = PythonOperator(
        task_id="notify_slack",
        python_callable=lambda: print("NOTIFICATION!"),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    run_smoke_test >> run_test >> slack_notification


class MyCustomOperator(BaseOperator):
    def execute(self, context: Context) -> Any:
        """
        Put your code here.
        """
