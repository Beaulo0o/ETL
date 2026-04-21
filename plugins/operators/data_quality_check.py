"""
Кастомный оператор для проверки качества данных.
"""
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class DataQualityCheckOperator(BaseOperator):
    """
    Оператор для выполнения проверок качества данных в PostgreSQL.

    Поддерживает:
    - Проверки на основе SQL запросов
    - Настраиваемые пороги допустимых значений
    - Детальное логирование результатов
    """

    template_fields = ("sql", "parameters")
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
            self,
            sql: str,
            expected_value: Any,
            postgres_conn_id: str = "postgres_default",
            parameters: Optional[Dict[str, Any]] = None,
            comparison_operator: str = "eq",
            tolerance: Optional[float] = None,
            fail_on_error: bool = True,
            *args,
            **kwargs,
    ) -> None:
        """
        Инициализация оператора.

        :param sql: SQL запрос для проверки (должен возвращать одно значение)
        :param expected_value: Ожидаемое значение
        :param postgres_conn_id: ID подключения к PostgreSQL
        :param parameters: Параметры для SQL запроса
        :param comparison_operator: Оператор сравнения ('eq', 'lt', 'gt', 'lte', 'gte', 'between')
        :param tolerance: Допустимое отклонение (для числовых значений)
        :param fail_on_error: Вызывать исключение при провале проверки
        """
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.expected_value = expected_value
        self.postgres_conn_id = postgres_conn_id
        self.parameters = parameters or {}
        self.comparison_operator = comparison_operator
        self.tolerance = tolerance
        self.fail_on_error = fail_on_error

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Выполнение проверки качества данных.

        :param context: Контекст выполнения Airflow
        :return: Результаты проверки
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Выполнение SQL запроса
        result = hook.get_first(self.sql, parameters=self.parameters)

        if result is None:
            actual_value = None
        elif len(result) == 1:
            actual_value = result[0]
        else:
            actual_value = result

        # Выполнение сравнения
        check_passed = self._compare_values(actual_value, self.expected_value)

        # Формирование результата
        check_result = {
            "task_id": self.task_id,
            "check_passed": check_passed,
            "actual_value": actual_value,
            "expected_value": self.expected_value,
            "comparison_operator": self.comparison_operator,
            "tolerance": self.tolerance,
            "sql": self.sql,
        }

        # Логирование
        if check_passed:
            logger.info(
                f"✅ Проверка '{self.task_id}' пройдена: {actual_value} {self._get_operator_symbol()} {self.expected_value}")
        else:
            error_msg = f"❌ Проверка '{self.task_id}' провалена: получено {actual_value}, ожидалось {self.expected_value}"
            logger.error(error_msg)

            if self.fail_on_error:
                raise ValueError(error_msg)

        # Сохранение результата в XCom
        context["ti"].xcom_push(key="data_quality_result", value=check_result)

        return check_result

    def _compare_values(self, actual: Any, expected: Any) -> bool:
        """
        Сравнение фактического и ожидаемого значений.

        :param actual: Фактическое значение
        :param expected: Ожидаемое значение
        :return: True если проверка пройдена
        """
        if actual is None:
            return False

        op = self.comparison_operator.lower()

        try:
            if op == "eq":
                if self.tolerance is not None and isinstance(actual, (int, float)):
                    return abs(actual - expected) <= self.tolerance
                return actual == expected

            elif op == "lt":
                return actual < expected

            elif op == "gt":
                return actual > expected

            elif op == "lte":
                return actual <= expected

            elif op == "gte":
                return actual >= expected

            elif op == "between":
                if not isinstance(expected, (list, tuple)) or len(expected) != 2:
                    raise ValueError("Для оператора 'between' ожидаемое значение должно быть списком из двух элементов")
                return expected[0] <= actual <= expected[1]

            else:
                raise ValueError(f"Неподдерживаемый оператор сравнения: {op}")

        except TypeError as e:
            logger.error(f"Ошибка сравнения типов: {e}")
            return False

    def _get_operator_symbol(self) -> str:
        """
        Получение символа оператора для логирования.
        """
        symbols = {
            "eq": "==",
            "lt": "<",
            "gt": ">",
            "lte": "<=",
            "gte": ">=",
            "between": "between",
        }
        return symbols.get(self.comparison_operator, self.comparison_operator)