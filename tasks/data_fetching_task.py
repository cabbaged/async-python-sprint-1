import logging
from queue import Queue

from external.client import YandexWeatherAPI
from utils import dump_data, get_url_by_city_name


class DataFetchingTask:
    @staticmethod
    def fetch_weather(city: str, output_queue: Queue):
        logging.info(f'Start fetching {city}')
        data_url = get_url_by_city_name(city)
        try:
            resp = YandexWeatherAPI.get_forecasting(data_url)
            if resp == {}:
                logging.info(f'No data for city {city}')
                return
            dump_data(f'{city}_weather.json', resp)
            output_queue.put(city)
            logging.info(f'Finish fetching {city}')
        except Exception as e:
            logging.info(f'Failed to fetch city {city} with error: {e}')
