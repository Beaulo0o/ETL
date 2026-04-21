"""
Тесты качества данных для трансформаций.
Используют Great Expectations для проверки качества данных.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import great_expectations as ge
from great_expectations.dataset import PandasDataset


class TestDataQuality:
    """Тесты качества данных."""

    @pytest.fixture
    def sample_sales_data(self):
        """Создание тестовых данных о продажах."""
        return pd.DataFrame({
            'transaction_id': ['TRX001', 'TRX002', 'TRX003'],
            'store_id': [1, 2, 3],
            'product_id': [101, 102, 103],
            'quantity': [2, 1, 3],
            'unit_price': [45.0, 89.0, 120.0],
            'transaction_date': [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=1),
                datetime.now()
            ]
        })

    @pytest.fixture
    def sample_sales_data_with_errors(self):
        """Создание тестовых данных с ошибками."""
        return pd.DataFrame({
            'transaction_id': ['TRX001', 'TRX002', None, 'TRX004'],
            'store_id': [1, None, 3, 4],
            'product_id': [101, 102, 103, 104],
            'quantity': [2, -1, 3, 10000],  # Отрицательное и аномальное значение
            'unit_price': [45.0, 89.0, -120.0, 100000],  # Отрицательная цена
            'transaction_date': [
                datetime.now() - timedelta(days=1),
                datetime.now() + timedelta(days=1),  # Будущая дата
                datetime.now(),
                datetime.now() - timedelta(days=365)
            ]
        })

    def test_no_null_required_columns(self, sample_sales_data):
        """Проверка отсутствия NULL в обязательных колонках."""
        ge_df = ge.from_pandas(sample_sales_data)

        required_columns = ['transaction_id', 'store_id', 'product_id']
        for col in required_columns:
            result = ge_df.expect_column_values_to_not_be_null(col)
            assert result.success, f"Колонка {col} содержит NULL значения"

    def test_positive_quantities_and_prices(self, sample_sales_data):
        """Проверка положительных значений количества и цены."""
        ge_df = ge.from_pandas(sample_sales_data)

        result_qty = ge_df.expect_column_values_to_be_between(
            'quantity', min_value=0, max_value=None
        )
        assert result_qty.success, "Обнаружены отрицательные значения quantity"

        result_price = ge_df.expect_column_values_to_be_between(
            'unit_price', min_value=0, max_value=None
        )
        assert result_price.success, "Обнаружены отрицательные значения unit_price"

    def test_transaction_dates_not_future(self, sample_sales_data):
        """Проверка отсутствия транзакций из будущего."""
        now = datetime.now()
        future_dates = sample_sales_data[
            sample_sales_data['transaction_date'] > now
        ]
        assert len(future_dates) == 0, \
            f"Найдено {len(future_dates)} транзакций из будущего"

    def test_detect_quality_issues(self, sample_sales_data_with_errors):
        """Проверка обнаружения проблем качества данных."""
        ge_df = ge.from_pandas(sample_sales_data_with_errors)

        # Проверка NULL значений
        null_check = ge_df.expect_column_values_to_not_be_null('transaction_id')
        assert not null_check.success, "NULL значения не обнаружены"

        # Проверка отрицательных значений
        qty_check = ge_df.expect_column_values_to_be_between(
            'quantity', min_value=0
        )
        assert not qty_check.success, "Отрицательные quantity не обнаружены"

        # Проверка аномальных значений
        qty_outlier_check = ge_df.expect_column_values_to_be_between(
            'quantity', max_value=1000
        )
        assert not qty_outlier_check.success, "Аномальные quantity не обнаружены"

    def test_calculate_total_amount(self, sample_sales_data):
        """Проверка расчета общей суммы."""
        df = sample_sales_data.copy()
        df['total_amount'] = df['quantity'] * df['unit_price']

        expected_totals = [90.0, 89.0, 360.0]  # 2*45, 1*89, 3*120
        calculated_totals = df['total_amount'].tolist()

        assert calculated_totals == expected_totals, \
            f"Неверный расчет total_amount: {calculated_totals} != {expected_totals}"

    def test_unique_transaction_ids(self, sample_sales_data):
        """Проверка уникальности transaction_id."""
        ge_df = ge.from_pandas(sample_sales_data)
        result = ge_df.expect_column_values_to_be_unique('transaction_id')
        assert result.success, "Обнаружены дубликаты transaction_id"


class TestDataTransformations:
    """Тесты трансформаций данных."""

    def test_clean_negative_quantities(self):
        """Тест очистки отрицательных значений quantity."""
        df = pd.DataFrame({
            'quantity': [5, -2, 10, -1, 3]
        })

        df_clean = df.copy()
        df_clean['quantity'] = df_clean['quantity'].clip(lower=0)

        expected = [5, 0, 10, 0, 3]
        assert df_clean['quantity'].tolist() == expected

    def test_add_audit_columns(self):
        """Тест добавления аудит колонок."""
        df = pd.DataFrame({'value': [1, 2, 3]})
        execution_time = datetime.now()

        df['etl_created_at'] = execution_time
        df['etl_source'] = 'test'
        df['etl_batch_id'] = 'batch_001'

        assert 'etl_created_at' in df.columns
        assert 'etl_source' in df.columns
        assert 'etl_batch_id' in df.columns
        assert len(df) == 3

    def test_aggregate_daily_sales(self):
        """Тест агрегации дневных продаж."""
        df = pd.DataFrame({
            'store_id': [1, 1, 2, 2, 1],
            'total_amount': [100, 200, 150, 50, 300],
            'transaction_id': ['T1', 'T2', 'T3', 'T4', 'T5']
        })

        daily_agg = df.groupby('store_id').agg({
            'total_amount': ['sum', 'mean', 'count'],
            'transaction_id': 'nunique'
        }).round(2)

        # Проверка для store_id = 1
        store1_agg = daily_agg.loc[1]
        assert store1_agg[('total_amount', 'sum')] == 600  # 100+200+300
        assert store1_agg[('total_amount', 'mean')] == 200  # 600/3
        assert store1_agg[('transaction_id', 'nunique')] == 3