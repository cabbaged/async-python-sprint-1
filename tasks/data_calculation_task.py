import logging
from queue import Queue

from external.analyzer import run_analyzer


class DataCalculationTask:
    @staticmethod
    def analyze_city(city: str, queue: Queue):
        logging.info(f'Start calculating {city}')
        run_analyzer(
            f'./artifacts/{city}_weather.json',
            f'./artifacts/{city}_analysis.json'
        )
        queue.put(city)
        logging.info(f'Finish calculating {city}')
