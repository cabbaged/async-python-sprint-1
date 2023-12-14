import logging

from external.analyzer import run_analyzer


class DataCalculationTask:
    def analyze_city(self, city, queue):
        logging.info(f'Start calculating {city}')
        run_analyzer(
            f'./artifacts/{city}_weather.json',
            f'./artifacts/{city}_analysis.json'
        )
        queue.put(city)
        logging.info(f'Finish calculating {city}')
