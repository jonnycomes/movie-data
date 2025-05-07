from pathlib import Path
from secret_settings import TMDB_API_KEY

# Define paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "movies.db"
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed"
DATA_PROCESSING_SQL_PATH = PROJECT_ROOT / "src" / "data_processing" / "sql"

# TMDB API settings
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_REQUEST_PAGE_LIMIT = 500
