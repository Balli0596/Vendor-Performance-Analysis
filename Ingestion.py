import os
import pandas as pd
import logging
import time
from sqlalchemy import create_engine

# ✅ Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ✅ Logs folder will always be created beside this script
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# ✅ Configure logging with absolute path
LOG_FILE = os.path.join(LOGS_DIR, "ingestion_db.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ✅ Database file will also be created beside the script
DB_PATH = os.path.join(BASE_DIR, "inventory.db")
engine = create_engine(f"sqlite:///{DB_PATH}")

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"✅ Table '{table_name}' created with {len(df)} rows")


def load_raw_data():
    start = time.time()
    folder = r"D:\data"   # raw string avoids escape issues

    for file in os.listdir(folder):
        if file.endswith('.csv'):
            file_path = os.path.join(folder, file)
            df = pd.read_csv(file_path)

            logging.info(f"Ingesting {file} into db")
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60

    logging.info("Ingestion complete")
    logging.info(f"Total Time taken {total_time:.2f} minutes")

if __name__ == "__main__":
    load_raw_data()
