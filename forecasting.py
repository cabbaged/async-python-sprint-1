import logging
import multiprocessing
from multiprocessing.managers import SyncManager
from multiprocessing.pool import ThreadPool
from queue import PriorityQueue

from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, create_artifacts_dir


class CustomManager(SyncManager):
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(module)s: %(message)s')
    create_artifacts_dir()

    CustomManager.register('PriorityQueue', PriorityQueue)

    manager = CustomManager()
    manager.start()
    fetched_cities = manager.Queue()
    calculated_cities = manager.Queue()
    aggregated_cities = manager.Queue()
    rating_queue = manager.PriorityQueue()

    active_tasks = []
    aggregation_result = {}

    with ThreadPool(processes=5) as thread_pool, multiprocessing.Pool() as process_pool:
        for city in CITIES:
            active_tasks.append(
                thread_pool.apply_async(DataFetchingTask().fetch_weather, (city, fetched_cities))
            )

        while active_tasks:
            while not fetched_cities.empty():
                fetched_city = fetched_cities.get(block=False)
                active_tasks.append(
                    process_pool.apply_async(DataCalculationTask().analyze_city, (fetched_city, calculated_cities))
                )

            while not calculated_cities.empty():
                calculated_city = calculated_cities.get(block=False)
                task = process_pool.apply_async(DataAggregationTask().aggregate, (calculated_city, aggregated_cities))
                active_tasks.append(task)
                aggregation_result[calculated_city] = task

            while not aggregated_cities.empty():
                aggregated_city, aggregated_city_data = aggregated_cities.get(block=False)
                active_tasks.append(
                    process_pool.apply_async(DataAnalyzingTask().analyze,
                                             (aggregated_city, aggregated_city_data, rating_queue))
                )

            active_tasks = [task for task in active_tasks if not task.ready()]

    DataAnalyzingTask().write_rating(rating_queue)
    manager.shutdown()
