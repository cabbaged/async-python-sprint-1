from tasks import DataFetchingTask
from queue import Queue
from unittest import mock
from external.client import YandexWeatherAPI

CITY_URL = {
    'https://code.s3.yandex.net/async-module/moscow-response.json': {'valid_data': 'valid_data'},
    'https://code.s3.yandex.net/async-module/paris-response.json': {}
}


def get_forecasting(url):
    return CITY_URL[url]


def test_data_fetching_task():
    queue = Queue()
    city = 'MOSCOW'
    with (mock.patch('tasks.data_fetching_task.dump_data') as mock_dump,
          mock.patch.object(YandexWeatherAPI, 'get_forecasting') as mock_api):
        mock_api.side_effect = get_forecasting
        DataFetchingTask.fetch_weather(city, queue)
        assert city == queue.get(block=False)
        mock_dump.assert_called_with(f'{city}_weather.json', {'valid_data': 'valid_data'})


def test_data_fetching_task_empty_data():
    queue = Queue()
    city = 'PARIS'
    with (mock.patch('tasks.data_fetching_task.dump_data') as mock_dump,
          mock.patch.object(YandexWeatherAPI, 'get_forecasting') as mock_api):
        mock_api.side_effect = get_forecasting
        DataFetchingTask.fetch_weather(city, queue)
        assert queue.empty()
        mock_dump.assert_not_called()


def test_data_fetching_task_negative_code():
    queue = Queue()
    city = 'PARIS'
    with (mock.patch('tasks.data_fetching_task.dump_data') as mock_dump,
          mock.patch.object(YandexWeatherAPI, 'get_forecasting') as mock_api):
        mock_api.side_effect = Exception('404')
        DataFetchingTask.fetch_weather(city, queue)
        assert queue.empty()
        mock_dump.assert_not_called()
