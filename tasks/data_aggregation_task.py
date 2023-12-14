import logging
from queue import Queue

from utils import load_data


class AggregateCalculator:
    def __init__(self):
        self.values = []

    def add(self, value):
        self.values.append(value)

    def mean(self):
        return sum(self.values) / len(self.values)

    def sum(self):
        return sum(self.values)


class DataAggregationTask:
    def aggregate(self, city: str, output_queue: Queue):
        logging.info(f'Start aggregating {city}')
        temperature_aggregator = AggregateCalculator()
        good_weather_hours_aggregator = AggregateCalculator()
        aggregate = {}
        data = load_data(f'{city}_analysis.json')
        for day in data['days']:
            temp_avg = day.get('temp_avg')
            good_weather_hours = day.get('relevant_cond_hours')
            date = day['date']
            if temp_avg is None or good_weather_hours is None:
                logging.info(f'Incomplete data for {city}, date {date}')
                continue

            aggregate[date] = {
                'average': temp_avg,
                'good_weather_hours': good_weather_hours
            }
            temperature_aggregator.add(temp_avg)
            good_weather_hours_aggregator.add(good_weather_hours)
        aggregate['good_weather_hours_for_period'] = good_weather_hours_aggregator.sum()
        aggregate['average_temp_for_period'] = temperature_aggregator.mean()
        output_queue.put((city, aggregate))
        logging.info(f'Finish aggregating {city}')
