from dotenv import load_dotenv

from pathlib import Path
import os 

if __name__ == "__main__":
    env_path = Path('.') / '.env'
    print(env_path)
    load_dotenv(dotenv_path=env_path)

    print(os.environ.get('DPP_DB_ENGINE'))