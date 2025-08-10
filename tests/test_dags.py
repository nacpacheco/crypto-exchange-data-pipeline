import pytest
from airflow.models import DagBag, Variable
from unittest.mock import patch

@pytest.fixture(scope="module")
def dagbag():
    with patch.object(Variable, "get", return_value="dummy_api_key"):
        yield DagBag(dag_folder="dags/", include_examples=False, read_dags_from_db=False)

def test_crypto_ingestion_dag_loaded(dagbag):
    dag = dagbag.dags["crypto_ingestion"]
    assert dag is not None
    assert len(dag.tasks) > 0
    assert "fetch_exchanges" in [t.task_id for t in dag.tasks]

def test_crypto_processing_dag_loaded(dagbag):
    dag = dagbag.dags["crypto_processing"]
    assert dag is not None
    assert len(dag.tasks) > 0
    assert "generate_exchanges_table" in [t.task_id for t in dag.tasks]