from tasks import DataAnalyzingTask
from queue import PriorityQueue
from .testing_utils import load_data


CITY_AVG_TEMP = 13.75
CITY_GOOD_WEATHER_HOURS = 8


def test_analyze():
    queue = PriorityQueue()
    city = 'MOSCOW'
    city_data = load_data(f'{city}_aggregate.json')
    DataAnalyzingTask.analyze(city, city_data, queue)
    key, value = queue.get(block=False)
    handled_city, handled_data = value
    assert key == (-CITY_AVG_TEMP, -CITY_GOOD_WEATHER_HOURS)
    assert city == handled_city
    assert city_data == handled_data


def test_write_data():
    queue = PriorityQueue()
    for city in ('PARIS', 'MOSCOW'):
        DataAnalyzingTask.analyze(city, load_data(f'{city}_aggregate.json'), queue)
    assert load_data('result.json') == DataAnalyzingTask().write_rating(queue)
