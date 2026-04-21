"""
Тесты целостности DAG файлов.
Проверяют, что все DAG файлы корректно импортируются и не содержат синтаксических ошибок.
"""
import pytest
from airflow.models import DagBag
import os
from pathlib import Path


class TestDagIntegrity:
    """Проверка целостности DAG файлов."""

    def setup_method(self):
        """Инициализация перед каждым тестом."""
        dags_path = Path(__file__).parent.parent.parent / "dags"
        self.dagbag = DagBag(dag_folder=str(dags_path), include_examples=False)

    def test_no_import_errors(self):
        """Проверка отсутствия ошибок импорта DAG файлов."""
        assert len(self.dagbag.import_errors) == 0, \
            f"Ошибки импорта DAG: {self.dagbag.import_errors}"

    def test_all_dags_have_default_args(self):
        """Проверка наличия default_args у всех DAGов."""
        for dag_id, dag in self.dagbag.dags.items():
            assert dag.default_args is not None, \
                f"DAG {dag_id} не имеет default_args"

            required_args = ['owner', 'depends_on_past', 'email_on_failure', 'retries']
            for arg in required_args:
                assert arg in dag.default_args, \
                    f"DAG {dag_id} не имеет '{arg}' в default_args"

    def test_all_dags_have_description(self):
        """Проверка наличия описания у всех DAGов."""
        for dag_id, dag in self.dagbag.dags.items():
            assert dag.description is not None and len(dag.description) > 0, \
                f"DAG {dag_id} не имеет описания"

    def test_all_dags_have_tags(self):
        """Проверка наличия тегов у всех DAGов."""
        for dag_id, dag in self.dagbag.dags.items():
            assert len(dag.tags) > 0, \
                f"DAG {dag_id} не имеет тегов"

    def test_dag_schedule_intervals(self):
        """Проверка корректности интервалов расписания."""
        for dag_id, dag in self.dagbag.dags.items():
            assert dag.schedule_interval is not None, \
                f"DAG {dag_id} не имеет schedule_interval"

    def test_required_dags_present(self):
        """Проверка наличия обязательных DAGов."""
        required_dags = [
            'weather_ingestion',
            'sales_transform',
            'reporting_daily'
        ]
        for dag_id in required_dags:
            assert dag_id in self.dagbag.dags, \
                f"Обязательный DAG '{dag_id}' отсутствует"


class TestDagStructure:
    """Проверка структуры DAGов."""

    def setup_method(self):
        """Инициализация перед каждым тестом."""
        dags_path = Path(__file__).parent.parent.parent / "dags"
        self.dagbag = DagBag(dag_folder=str(dags_path), include_examples=False)

    def test_weather_ingestion_structure(self):
        """Проверка структуры DAG weather_ingestion."""
        dag = self.dagbag.get_dag('weather_ingestion')
        assert dag is not None

        task_ids = [task.task_id for task in dag.tasks]
        expected_tasks = ['extract_weather_data', 'save_raw_to_s3']

        for task_id in expected_tasks:
            assert task_id in task_ids, \
                f"Задача '{task_id}' отсутствует в DAG weather_ingestion"

    def test_sales_transform_structure(self):
        """Проверка структуры DAG sales_transform."""
        dag = self.dagbag.get_dag('sales_transform')
        assert dag is not None

        task_ids = [task.task_id for task in dag.tasks]
        expected_tasks = [
            'extract_staging_sales',
            'transform_to_ods',
            'load_to_ods',
            'refresh_sales_mart',
            'run_data_quality_checks'
        ]

        for task_id in expected_tasks:
            assert task_id in task_ids, \
                f"Задача '{task_id}' отсутствует в DAG sales_transform"

    def test_reporting_daily_structure(self):
        """Проверка структуры DAG reporting_daily."""
        dag = self.dagbag.get_dag('reporting_daily')
        assert dag is not None

        task_ids = [task.task_id for task in dag.tasks]
        expected_tasks = [
            'generate_sales_report',
            'export_to_excel_s3',
            'send_notification'
        ]

        for task_id in expected_tasks:
            assert task_id in task_ids, \
                f"Задача '{task_id}' отсутствует в DAG reporting_daily"


class TestDagDependencies:
    """Проверка зависимостей между задачами в DAGах."""

    def setup_method(self):
        """Инициализация перед каждым тестом."""
        dags_path = Path(__file__).parent.parent.parent / "dags"
        self.dagbag = DagBag(dag_folder=str(dags_path), include_examples=False)

    def test_weather_ingestion_dependencies(self):
        """Проверка зависимостей в DAG weather_ingestion."""
        dag = self.dagbag.get_dag('weather_ingestion')

        extract_task = dag.get_task('extract_weather_data')
        save_task = dag.get_task('save_raw_to_s3')

        # Проверка, что save_task зависит от extract_task
        assert save_task in extract_task.downstream_list, \
            "save_raw_to_s3 должна быть downstream от extract_weather_data"

    def test_sales_transform_dependencies(self):
        """Проверка зависимостей в DAG sales_transform."""
        dag = self.dagbag.get_dag('sales_transform')

        transform_task = dag.get_task('transform_to_ods')
        load_task = dag.get_task('load_to_ods')
        mart_task = dag.get_task('refresh_sales_mart')
        dq_task = dag.get_task('run_data_quality_checks')

        # Проверка цепочки зависимостей
        assert load_task in transform_task.downstream_list
        assert mart_task in load_task.downstream_list
        assert dq_task in mart_task.downstream_list