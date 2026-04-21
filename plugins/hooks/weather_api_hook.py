"""
Кастомный хук для работы с OpenWeatherMap API.
"""
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import requests
from typing import Dict, Any, Optional, List
import time
import logging

logger = logging.getLogger(__name__)


class WeatherAPIHook(BaseHook):
    """
    Хук для взаимодействия с OpenWeatherMap API.

    Поддерживает:
    - Current weather data
    - 5 day forecast
    - Rate limit handling
    """

    BASE_URL = "http://api.openweathermap.org/data/2.5"

    def __init__(
            self,
            conn_id: str = "openweather_default",
            api_key: Optional[str] = None,
            timeout: int = 10,
            retries: int = 3,
    ) -> None:
        """
        Инициализация хука.

        :param conn_id: ID подключения в Airflow (опционально)
        :param api_key: API ключ (если не используется conn_id)
        :param timeout: Таймаут запроса в секундах
        :param retries: Количество повторных попыток при ошибке
        """
        super().__init__()
        self.conn_id = conn_id
        self._api_key = api_key
        self.timeout = timeout
        self.retries = retries

    def _get_api_key(self) -> str:
        """
        Получение API ключа из подключения или переменной.
        """
        if self._api_key:
            return self._api_key

        try:
            conn = self.get_connection(self.conn_id)
            if conn.password:
                return conn.password
            elif conn.extra_dejson.get("api_key"):
                return conn.extra_dejson["api_key"]
        except Exception as e:
            logger.warning(f"Не удалось получить подключение {self.conn_id}: {e}")

        from airflow.models import Variable
        api_key = Variable.get("OPENWEATHER_API_KEY", default_var=None)
        if not api_key:
            raise AirflowException("API ключ не найден ни в подключении, ни в переменных")

        return api_key

    def get_current_weather(self, city: str, units: str = "metric", lang: str = "ru") -> Dict[str, Any]:
        """
        Получение текущей погоды для указанного города.

        :param city: Название города
        :param units: Единицы измерения (metric/imperial)
        :param lang: Язык ответа
        :return: Словарь с данными о погоде
        """
        endpoint = f"{self.BASE_URL}/weather"
        api_key = self._get_api_key()

        params = {
            "q": city,
            "appid": api_key,
            "units": units,
            "lang": lang,
        }

        return self._make_request(endpoint, params)

    def get_forecast(self, city: str, cnt: int = 5, units: str = "metric") -> Dict[str, Any]:
        """
        Получение прогноза погоды на несколько дней.

        :param city: Название города
        :param cnt: Количество временных отрезков (макс 40)
        :param units: Единицы измерения
        :return: Словарь с прогнозом
        """
        endpoint = f"{self.BASE_URL}/forecast"
        api_key = self._get_api_key()

        params = {
            "q": city,
            "appid": api_key,
            "cnt": min(cnt, 40),
            "units": units,
        }

        return self._make_request(endpoint, params)

    def get_weather_by_coords(self, lat: float, lon: float, units: str = "metric") -> Dict[str, Any]:
        """
        Получение погоды по координатам.

        :param lat: Широта
        :param lon: Долгота
        :param units: Единицы измерения
        :return: Словарь с данными о погоде
        """
        endpoint = f"{self.BASE_URL}/weather"
        api_key = self._get_api_key()

        params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "units": units,
        }

        return self._make_request(endpoint, params)

    def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Выполнение HTTP запроса с обработкой ошибок и повторными попытками.

        :param url: URL эндпоинта
        :param params: Параметры запроса
        :return: JSON ответ
        """
        for attempt in range(self.retries):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()

                # Обработка rate limit
                if response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limit превышен. Ожидание {wait_time} секунд...")
                    time.sleep(wait_time)
                    continue

                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Попытка {attempt + 1}/{self.retries} не удалась: {e}")

                if attempt == self.retries - 1:
                    raise AirflowException(f"Не удалось выполнить запрос после {self.retries} попыток: {e}")

                # Экспоненциальная задержка
                wait_time = 2 ** attempt
                logger.info(f"Ожидание {wait_time} секунд перед повторной попыткой...")
                time.sleep(wait_time)

        raise AirflowException("Не удалось выполнить запрос")

    def test_connection(self) -> tuple[bool, str]:
        """
        Тестирование подключения к API.

        :return: (success, message)
        """
        try:
            self.get_current_weather("Moscow")
            return True, "Успешное подключение к OpenWeatherMap API"
        except Exception as e:
            return False, f"Ошибка подключения: {e}"