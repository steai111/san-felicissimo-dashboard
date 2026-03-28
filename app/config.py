# File: config.py

from pathlib import Path
import os

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


PROJECT_NAME = os.getenv("PROJECT_NAME", "Data_Dashboard")
BASE_DIR = Path(os.getenv("BASE_DIR", str(PROJECT_ROOT)))

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))

DB_PATH = Path(os.getenv("DB_PATH", str(DATA_DIR / "database/dashboard.db")))
RAW_CSV_DIR = Path(os.getenv("RAW_CSV_DIR", str(DATA_DIR / "raw_csv")))
RAW_TABLEAU_DIR = Path(os.getenv("RAW_TABLEAU_DIR", str(DATA_DIR / "raw_tableau")))
RAW_TABLEAU_UNITS_DIR = Path(os.getenv("RAW_TABLEAU_UNITS_DIR", str(DATA_DIR / "raw_tableau_units")))
RAW_GA4_DIR = Path(os.getenv("RAW_GA4_DIR", str(DATA_DIR / "raw_ga4")))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", str(DATA_DIR / "processed")))
LOGS_DIR = Path(os.getenv("LOGS_DIR", str(DATA_DIR / "logs")))

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

# EOF - config.py