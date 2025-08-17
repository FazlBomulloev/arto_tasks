import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# === Telegram Bot ===
BOT_TOKEN = os.getenv('BOT_TOKEN', '8180256473:AAHrypuOVD2vEFMcNvhFS4lXzrmeZ9XYtCw')

# === Telegram API ===
API_ID = int(os.getenv('API_ID', '12345'))
API_HASH = os.getenv('API_HASH', '0123456789abcdef0123456789abcdef')

# === Database ===
DB_CONFIG = {
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', Path('vars/db_password.txt').read_text().strip()),
    'database': os.getenv('DB_NAME', 'arto_db'),
    'host': os.getenv('DB_HOST', '92.255.76.235'),
    'port': os.getenv('DB_PORT', '5432')
}

# === Redis ===
REDIS_HOST = os.getenv('REDIS_HOST', )
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', Path('vars/password.txt').read_text().strip())

# === Session Management ===
MAX_SESSIONS_IN_MEMORY = int(os.getenv('MAX_SESSIONS', '25000'))
SESSION_CACHE_TTL = int(os.getenv('SESSION_TTL', '3600'))  # 1 час

def get_view_task_duration() -> int:
    """Получает длительность просмотров из настроек (в секундах)"""
    hours = read_setting('followPeriod.txt')  # По умолчанию 1 час
    return int(hours * 3600)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

# === File Paths ===
VARS_DIR = Path('vars')
ACCOUNTS_DIR = Path('accounts')
DOWNLOADS_DIR = Path('downloads')
LOGS_DIR = Path('logs')

# Создаем директории если их нет
for dir_path in [ACCOUNTS_DIR, DOWNLOADS_DIR, LOGS_DIR]:
    dir_path.mkdir(exist_ok=True)

# === Whitelist ===
def get_whitelist():
    try:
        whitelist_file = VARS_DIR / 'whitelist.txt'
        if whitelist_file.exists():
            return whitelist_file.read_text().strip().split(',')
        return []
    except:
        return []

def read_setting(filename: str, default: float = 0.0) -> float:
    """Читает настройку из файла vars/"""
    try:
        file_path = VARS_DIR / filename
        if file_path.exists():
            return float(file_path.read_text().strip())
        return default
    except:
        return default

def write_setting(filename: str, value: str):
    """Записывает настройку в файл vars/"""
    try:
        file_path = VARS_DIR / filename
        file_path.write_text(str(value))
    except Exception as e:
        print(f"Error writing setting {filename}: {e}")

# === Language Utils ===
def load_languages():
    """Загружает языковые файлы"""
    try:
        ru_langs = (VARS_DIR / 'langsRu.txt').read_text(encoding='utf-8').split(', ')
        en_langs = (VARS_DIR / 'langsEn.txt').read_text(encoding='utf-8').split(', ')
        lang_codes = (VARS_DIR / 'langsCode.txt').read_text(encoding='utf-8').split(', ')
        
        return {
            'ru': ru_langs,
            'en': en_langs,
            'codes': lang_codes
        }
    except Exception as e:
        print(f"Error loading languages: {e}")
        return {'ru': [], 'en': [], 'codes': []}

def find_english_word(russian_word: str) -> str:
    """Находит английский эквивалент русского языка"""
    langs = load_languages()
    try:
        index = langs['ru'].index(russian_word)
        return langs['en'][index]
    except (ValueError, IndexError):
        return russian_word

def find_russian_word(english_word: str) -> str:
    """Находит русский эквивалент английского языка"""
    langs = load_languages()
    try:
        index = langs['en'].index(english_word)
        return langs['ru'][index]
    except (ValueError, IndexError):
        return english_word

def find_lang_code(english_word: str) -> str:
    """Находит код языка по английскому названию"""
    langs = load_languages()
    try:
        index = langs['en'].index(english_word)
        return langs['codes'][index]
    except (ValueError, IndexError):
        return 'en'

# === Logging Config ===
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOGS_DIR / 'bot.log',
            'maxBytes': 10 * 1024 * 1024,  # 10MB
            'backupCount': 3,
            'encoding': 'utf-8'
        },
        'console': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'worker': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOGS_DIR / 'worker.log',
            'maxBytes': 50 * 1024 * 1024,  # 50MB
            'backupCount': 2,
            'encoding': 'utf-8'
        }
    },
    'loggers': {
        '': {
            'handlers': ['default', 'console'],
            'level': 'INFO',
            'propagate': False
        },
        'worker': {
            'handlers': ['worker', 'console'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

RUN_WORKER = os.getenv('RUN_WORKER', 'true').lower() == 'true'
RUN_BOT = os.getenv('RUN_BOT', 'true').lower() == 'true'

# === Development/Production ===
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
