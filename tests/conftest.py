"""
Конфигурация pytest для проекта.
Содержит фикстуры и настройки для тестирования.
"""
import pytest
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Добавление путей проекта в PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "dags"))
sys.path.insert(0, str(project_root / "plugins"))

# Установка переменных окружения для тестов
os.environ["AIRFLOW_HOME"] = str(project_root)
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(project_root / "dags")


@pytest.fixture(scope="session")
def airflow_home():
    """Фикстура для AIRFLOW_HOME."""
    return project_root


@pytest.fixture(scope="function")
def mock_postgres_hook():
    """Мок для PostgresHook."""
    with patch('airflow.providers.postgres.hooks.postgres.PostgresHook') as mock:
        hook_instance = MagicMock()
        mock.return_value = hook_instance
        yield hook_instance


@pytest.fixture(scope="function")
def mock_s3_hook():
    """Мок для S3Hook."""
    with patch('airflow.providers.amazon.aws.hooks.s3.S3Hook') as mock:
        hook_instance = MagicMock()
        mock.return_value = hook_instance
        yield hook_instance


@pytest.fixture(scope="function")
def mock_variable_get():
    """Мок для Variable.get()."""
    with patch('airflow.models.Variable.get') as mock:
        mock.return_value = "test_value"
        yield mock


@pytest.fixture(scope="function")
def mock_requests_get():
    """Мок для requests.get."""
    with patch('requests.get') as mock:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "Moscow",
            "main": {"temp": 20.5, "humidity": 65},
            "weather": [{"main": "Clear", "description": "clear sky"}]
        }
        mock.return_value = mock_response
        yield mock


@pytest.fixture(scope="function")
def dag_bag():
    """Фикстура для DagBag."""
    from airflow.models import DagBag
    dags_path = project_root / "dags"
    return DagBag(dag_folder=str(dags_path), include_examples=False)


@pytest.fixture(scope="function")
def test_dag(dag_bag):
    """Фикстура для получения тестового DAG."""
    return dag_bag.get_dag('weather_ingestion')