-- =====================================================
-- Набор SQL проверок качества данных
-- Используется в DataQualityCheckOperator
-- =====================================================

-- Проверка 1: Отсутствие отрицательных сумм продаж
-- Ожидаемый результат: 0
SELECT COUNT(*) AS negative_amounts_count
FROM ods.sales
WHERE total_amount < 0
  AND DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day';

-- Проверка 2: Отсутствие NULL в обязательных полях
SELECT COUNT(*) AS null_store_ids
FROM ods.sales
WHERE store_id IS NULL
  AND DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day';

-- Проверка 3: Отсутствие транзакций из будущего
SELECT COUNT(*) AS future_transactions
FROM ods.sales
WHERE transaction_date > CURRENT_TIMESTAMP;

-- Проверка 4: Целостность связей с справочниками
SELECT COUNT(*) AS orphaned_store_records
FROM ods.sales s
LEFT JOIN ods.stores st ON s.store_id = st.store_id
WHERE st.store_id IS NULL
  AND DATE(s.transaction_date) = CURRENT_DATE - INTERVAL '1 day';

-- Проверка 5: Целостность связей с товарами
SELECT COUNT(*) AS orphaned_product_records
FROM ods.sales s
LEFT JOIN ods.products p ON s.product_id = p.product_id
WHERE p.product_id IS NULL
  AND DATE(s.transaction_date) = CURRENT_DATE - INTERVAL '1 day';

-- Проверка 6: Аномальные значения количества товаров (> 1000 единиц в одной позиции)
SELECT COUNT(*) AS suspicious_quantities
FROM ods.sales
WHERE quantity > 1000
  AND DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day';

-- Проверка 7: Аномально высокие чеки (> 50000)
SELECT COUNT(*) AS suspicious_receipts
FROM ods.sales
WHERE total_amount > 50000
  AND DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day';

-- Проверка 8: Количество записей за день (должно быть > 0 для рабочих дней)
SELECT
    CASE
        WHEN COUNT(*) > 0 THEN 1
        ELSE 0
    END AS has_data_for_yesterday
FROM ods.sales
WHERE DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day';