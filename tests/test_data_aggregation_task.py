from tasks import DataAggregationTask
from queue import Queue
from unittest import mock
from .testing_utils import load_data


def test_data_fetching_task():
    queue = Queue()
    city = 'MOSCOW'
    with mock.patch('tasks.data_aggregation_task.load_data') as load_mock:
        load_mock.side_effect = load_data
        DataAggregationTask.aggregate(city, queue)
        handled_city, city_data = queue.get(block=False)
        assert handled_city == city
        assert city_data == load_data(f'{city}_aggregate.json')
