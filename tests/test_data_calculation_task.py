from tasks import DataCalculationTask
from queue import Queue
from unittest import mock


def test_data_fetching_task():
    queue = Queue()
    city = 'MOSCOW'
    with mock.patch('tasks.data_calculation_task.run_analyzer') as analyzer_mock:
        DataCalculationTask().analyze_city(city, queue)
        assert city == queue.get(block=False)
        analyzer_mock.assert_called_with(
            f'./artifacts/{city}_weather.json',
            f'./artifacts/{city}_analysis.json'
        )
