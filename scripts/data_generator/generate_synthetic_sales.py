#!/usr/bin/env python3
"""
Генератор синтетических данных о продажах для тестирования ETL пайплайна.
"""
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesDataGenerator:
    """Генератор синтетических данных о розничных продажах."""

    STORES = [
        {"id": 1, "name": "Центральный", "city": "Moscow", "type": "supermarket"},
        {"id": 2, "name": "Северный", "city": "Saint Petersburg", "type": "hypermarket"},
        {"id": 3, "name": "Южный", "city": "Novosibirsk", "type": "convenience"},
        {"id": 4, "name": "Западный", "city": "Yekaterinburg", "type": "supermarket"},
        {"id": 5, "name": "Восточный", "city": "Kazan", "type": "supermarket"},
        {"id": 6, "name": "Приморский", "city": "Moscow", "type": "convenience"},
        {"id": 7, "name": "Горный", "city": "Yekaterinburg", "type": "hypermarket"},
    ]

    PRODUCTS = [
        {"id": 101, "name": "Хлеб", "category": "Бакалея", "base_price": 45.0},
        {"id": 102, "name": "Молоко", "category": "Молочные продукты", "base_price": 89.0},
        {"id": 103, "name": "Яйца (10 шт)", "category": "Молочные продукты", "base_price": 120.0},
        {"id": 104, "name": "Курица", "category": "Мясо", "base_price": 250.0},
        {"id": 105, "name": "Говядина", "category": "Мясо", "base_price": 550.0},
        {"id": 106, "name": "Яблоки", "category": "Фрукты", "base_price": 120.0},
        {"id": 107, "name": "Бананы", "category": "Фрукты", "base_price": 90.0},
        {"id": 108, "name": "Вода 1.5л", "category": "Напитки", "base_price": 55.0},
        {"id": 109, "name": "Сок апельсиновый", "category": "Напитки", "base_price": 150.0},
        {"id": 110, "name": "Шоколад", "category": "Кондитерские изделия", "base_price": 85.0},
    ]

    def __init__(self, seed: int = 42):
        """Инициализация генератора."""
        np.random.seed(seed)
        random.seed(seed)
        self.transaction_counter = 1

    def generate_transactions(
            self, date: datetime, num_transactions: int = 1000
    ) -> pd.DataFrame:
        """
        Генерация транзакций за указанную дату.

        :param date: Дата генерации
        :param num_transactions: Количество транзакций
        :return: DataFrame с транзакциями
        """
        transactions = []

        # Определение дня недели (влияет на количество покупок)
        is_weekend = date.weekday() >= 5

        # Определяем максимальный час (чтобы не генерить будущее)
        now = datetime.now()
        if date.date() == now.date():
            max_hour = min(22, now.hour)
            if max_hour < 8:
                max_hour = 8
        else:
            max_hour = 22

        for _ in range(num_transactions):
            # Выбор случайного магазина
            store = random.choice(self.STORES)

            # Количество позиций в чеке (больше в выходные)
            items_count = np.random.poisson(lam=3 if is_weekend else 2)
            items_count = max(1, min(items_count, 10))

            # Время транзакции
            hour_probs = self._get_hourly_distribution(is_weekend, max_hour)
            hour = np.random.choice(
                range(8, max_hour + 1),
                p=hour_probs
            )
            transaction_time = date.replace(
                hour=hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
                tzinfo=None  # без часового пояса, будет как UTC
            )

            for _ in range(items_count):
                product = random.choice(self.PRODUCTS)

                # Вариация цены (±10%)
                price_variation = np.random.uniform(0.9, 1.1)
                unit_price = round(product["base_price"] * price_variation, 2)

                # Количество (обычно 1-3, редко больше)
                quantity = np.random.choice(
                    [1, 1, 1, 1, 2, 2, 3, 4],
                    p=[0.5, 0.2, 0.1, 0.05, 0.05, 0.04, 0.03, 0.03]
                )

                transactions.append({
                    "transaction_id": f"TRX{date.strftime('%Y%m%d')}{self.transaction_counter:06d}",
                    "store_id": store["id"],
                    "product_id": product["id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "transaction_date": transaction_time,
                })

            self.transaction_counter += 1

        return pd.DataFrame(transactions)

    def _get_hourly_distribution(self, is_weekend: bool, max_hour: int = 22):
        """
        Распределение вероятности покупок по часам.
        :param is_weekend: Флаг выходного дня
        :param max_hour: Максимальный час (чтобы не генерить будущее)
        :return: Список вероятностей для каждого часа от 8 до max_hour
        """
        if is_weekend:
            # Выходные: пик днем
            probs = []
            for hour in range(24):
                if hour < 8:  # Ночь/раннее утро
                    prob = 0.01
                elif hour < 12:  # Утро
                    prob = 0.05
                elif hour < 18:  # День
                    prob = 0.08
                elif hour < 22:  # Вечер
                    prob = 0.06
                else:  # Ночь
                    prob = 0.02
                probs.append(prob)
        else:
            # Будни: пик утром и вечером
            probs = []
            for hour in range(24):
                if hour < 7:  # Ночь
                    prob = 0.005
                elif hour < 10:  # Утренний пик
                    prob = 0.10
                elif hour < 17:  # Рабочее время
                    prob = 0.03
                elif hour < 21:  # Вечерний пик
                    prob = 0.12
                elif hour < 23:  # Поздний вечер
                    prob = 0.04
                else:  # Ночь
                    prob = 0.005
                probs.append(prob)

        # Обрезаем до max_hour (от 8 до max_hour)
        probs = probs[8:max_hour + 1]

        # Нормализация
        total = sum(probs)
        if total == 0:
            # Если все нули — равномерное распределение
            return [1.0 / len(probs)] * len(probs)
        return [p / total for p in probs]

    def insert_to_postgres(self, df: pd.DataFrame):
        """Вставка данных в PostgreSQL staging таблицу."""
        try:
            from sqlalchemy import create_engine

            # Получаем параметры подключения из переменных окружения
            pg_host = os.getenv("POSTGRES_HOST", "postgres")
            pg_port = os.getenv("POSTGRES_PORT", "5432")
            pg_user = os.getenv("POSTGRES_USER", "airflow")
            pg_password = os.getenv("POSTGRES_PASSWORD", "airflow")
            pg_db = os.getenv("POSTGRES_DB", "retail_db")

            engine = create_engine(
                f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
            )

            # Добавляем аудит колонки
            df["_imported_at"] = datetime.now()
            df["_source_file"] = "synthetic_generator"
            df["_batch_id"] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # Вставка в staging.sales
            df.to_sql(
                name="sales",
                schema="staging",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            logger.info(f"✅ Успешно вставлено {len(df)} записей в staging.sales")

        except ImportError:
            logger.warning("sqlalchemy не установлен. Данные не будут вставлены в БД.")
            logger.info(f"Сгенерировано {len(df)} записей (только в памяти)")
        except Exception as e:
            logger.error(f"Ошибка при вставке в PostgreSQL: {e}")
            raise


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description="Генератор синтетических данных о продажах")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Количество дней для генерации (по умолчанию: 30)"
    )
    parser.add_argument(
        "--transactions-per-day",
        type=int,
        default=1000,
        help="Количество транзакций в день (по умолчанию: 1000)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Начальная дата в формате YYYY-MM-DD (по умолчанию: сегодня - days)"
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Не вставлять данные в базу данных"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Сохранить в CSV файл (опционально)"
    )

    args = parser.parse_args()

    generator = SalesDataGenerator()

    # Определяем начальную дату
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = datetime.now() - timedelta(days=args.days)

    all_data = []

    logger.info(f"🚀 Генерация данных за {args.days} дней, начиная с {start_date.date()}")

    for i in range(args.days):
        current_date = start_date + timedelta(days=i)
        logger.info(f"📅 Генерация данных за {current_date.date()}")

        df = generator.generate_transactions(
            date=current_date,
            num_transactions=args.transactions_per_day
        )

        if not args.no_db:
            generator.insert_to_postgres(df)

        all_data.append(df)

    # Объединяем все данные
    full_df = pd.concat(all_data, ignore_index=True)

    logger.info(f"✅ Всего сгенерировано {len(full_df)} записей")

    # Сохраняем в CSV если нужно
    if args.output:
        full_df.to_csv(args.output, index=False)
        logger.info(f"📁 Данные сохранены в {args.output}")

    # Статистика
    logger.info("\n📊 Статистика:")
    logger.info(f"   - Всего транзакций: {full_df['transaction_id'].nunique()}")
    logger.info(f"   - Всего позиций: {len(full_df)}")
    logger.info(f"   - Общая сумма продаж: {(full_df['quantity'] * full_df['unit_price']).sum():,.2f} ₽")
    logger.info(
        f"   - Средний чек: {full_df.groupby('transaction_id').apply(lambda x: (x['quantity'] * x['unit_price']).sum()).mean():,.2f} ₽")


if __name__ == "__main__":
    main()