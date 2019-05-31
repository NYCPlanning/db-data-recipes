from pathlib import Path
import datetime

def create_csv_path(path):
    new_path = Path(path)/ f'{datetime.date.today().isoformat()}'
    return str(new_path)

def create_base_path(path):
    base_path = create_csv_path(Path(path).parent.relative_to(Path(path).cwd()))
    return str(base_path)