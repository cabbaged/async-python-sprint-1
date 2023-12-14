from pathlib import Path
import json

DATA_PATH = Path(__file__).parent / 'data'


def load_data(file):
    path = DATA_PATH / file
    return json.loads(path.read_text())
