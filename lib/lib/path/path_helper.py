from pathlib import Path
import datetime

def create_csv_path(path):
    new_path = Path(path)/ f'{datetime.date.today().isoformat()}'
    return str(new_path)