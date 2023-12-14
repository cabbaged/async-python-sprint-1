import logging

from utils import dump_data


class DataAnalyzingTask:
    def analyze(self, city, city_data, priority_queue):
        logging.info(f'Start analyzing {city}')
        priority_queue.put((
            (-city_data['average_temp_for_period'], -city_data['good_weather_hours_for_period']),
            (city, city_data)
        ))

    def write_rating(self, priority_queue):
        logging.info('Start writing')
        prev_key = None
        rating = 0
        res = {}
        best_cities = []
        while not priority_queue.empty():
            key, value = priority_queue.get()

            if not (prev_key and key == prev_key):
                rating += 1
            prev_key = key
            city, city_data = value
            res[city] = city_data
            res[city]['rating'] = rating
            logging.info(f'City-rating {city} {rating}')
            if rating == 1:
                best_cities.append(city)

        logging.info(f'Best cities: {best_cities}')
        logging.info('End writing')
        dump_data('result.json', res)
        logging.info('Check results in artifacts/result.json file')
        return res
