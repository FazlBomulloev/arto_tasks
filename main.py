import asyncio
import logging
import logging.config
from aiogram import Bot, Dispatcher
from pathlib import Path

from config import BOT_TOKEN, LOGGING_CONFIG, RUN_WORKER, RUN_BOT, VARS_DIR
from database import init_db_pool, create_tables, shutdown_db_pool
from handlers import get_all_routers
from worker import TaskWorker

# Настройка логирования
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

def ensure_vars_structure():
    """Проверяет и создает структуру vars/ при необходимости"""
    
    # Создаем папку vars если её нет
    VARS_DIR.mkdir(exist_ok=True)
    
    # Критически важные файлы с дефолтными значениями
    critical_files = {
        'followPeriod.txt': '10',      # Период просмотров (часы)
        'delay.txt': '5',              # Задержка просмотров (минуты)  
        'lag.txt': '30',               # Основная задержка подписок (минуты)
        'range.txt': '5',              # Разброс подписок (минуты)
        'accounts_delay.txt': '10',    # Задержка между аккаунтами (минуты)
        'timeout_count.txt': '4',      # Количество подписок до таймаута
        'timeout_duration.txt': '20',  # Длительность таймаута (минуты)
        'reset_delay.txt': '360',      # Основная задержка сброса сессий (секунды)
        'reset_range.txt': '60',       # Вторичная задержка сброса (секунды)
        'whitelist.txt': '988710227,7059634482,817411344',  # ID пользователей
        'password.txt': '5f03f8eeb9:fx'  # Пароль БД (замените на свой!)
    }
    
    # Языковые файлы
    language_files = {
        'langsRu.txt': 'Английский, Русский, Испанский, Португальский, Немецкий, Французский, Итальянский, Китайский, Японский, Корейский, Арабский, Турецкий, Персидский, Индонезийский, Вьетнамский, Тайский, Малайский, Хинди, Бенгальский, Украинский, Польский, Нидерландский, Шведский, Финский, Датский, Норвежский, Румынский, Венгерский, Чешский, Греческий, Каталанский, Сербский, Болгарский, Хорватский, Словенский, Словацкий, Эстонский, Латышский, Литовский, Македонский, Африканский, Филиппинский, Грузинский, Азербайджанский, Узбекский, Казахский, Татарский',
        
        'langsEn.txt': 'English, Russian, Spanish, Portuguese, German, French, Italian, Chinese, Japanese, Korean, Arabic, Turkish, Persian, Indonesian, Vietnamese, Thai, Malay, Hindi, Bengali, Ukrainian, Polish, Dutch, Swedish, Finnish, Danish, Norwegian, Romanian, Hungarian, Czech, Greek, Catalan, Serbian, Bulgarian, Croatian, Slovenian, Slovak, Estonian, Latvian, Lithuanian, Macedonian, Afrikaans, Filipino, Georgian, Azerbaijani, Uzbek, Kazakh, Tatar',
        
        'langsCode.txt': 'en, ru, es, pt, de, fr, it, zh, ja, ko, ar, tr, fa, id, vi, th, ms, hi, bn, uk, pl, nl, sv, fi, da, no, ro, hu, cs, el, ca, sr, bg, hr, sl, sk, et, lv, lt, mk, af, ru, ka, az, uz, kk, ru'
    }
    
    # Объединяем все файлы
    all_files = {**critical_files, **language_files}
    
    created_files = []
    
    for filename, content in all_files.items():
        file_path = VARS_DIR / filename
        
        if not file_path.exists():
            try:
                file_path.write_text(content, encoding='utf-8')
                created_files.append(filename)
            except Exception as e:
                logger.error(f"❌ Не удалось создать {filename}: {e}")
    
    if created_files:
        logger.info(f"📁 Создано файлов настроек: {len(created_files)}")
        logger.info("⚠️  ВАЖНО: Проверьте файл vars/password.txt и обновите его!")
        logger.info("⚠️  ВАЖНО: Добавьте свой ID в vars/whitelist.txt")
    
    # Создаем дополнительные папки
    additional_dirs = ['accounts', 'downloads', 'logs']
    for dir_name in additional_dirs:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            dir_path.mkdir(exist_ok=True)
            logger.info(f"📁 Создана папка: {dir_name}/")

async def main():
    """Главная функция - запуск бота и/или воркера"""
    
    # Проверяем структуру файлов перед запуском
    ensure_vars_structure()
    
    if not RUN_BOT and not RUN_WORKER:
        logger.error("❌ И бот, и воркер отключены в конфигурации!")
        return
    
    mode_description = []
    if RUN_BOT:
        mode_description.append("🤖 Бот")
    if RUN_WORKER:
        mode_description.append("🔧 Воркер")
    
    logger.info(f"🚀 Запуск: {' + '.join(mode_description)}")
    
    try:
        # Устанавливаем uvloop если доступен
        try:
            import uvloop
            uvloop.install()
            logger.info("✅ uvloop установлен")
        except ImportError:
            logger.info("⚠️ uvloop недоступен")
        
        # Инициализация базы данных (нужна всегда)
        await init_db_pool()
        await create_tables()
        logger.info("✅ База данных инициализирована")
        
        # Подготавливаем задачи для запуска
        tasks = []
        bot = None
        worker = None
        
        # Инициализация бота
        if RUN_BOT:
            bot = Bot(token=BOT_TOKEN)
            dp = Dispatcher()
            
            # Подключение роутеров
            routers = get_all_routers()
            dp.include_routers(*routers)
            logger.info(f"✅ Подключено {len(routers)} роутеров")
            
            async def run_bot():
                logger.info("🤖 Запуск Telegram бота...")
                logger.info("📝 Доступные команды: /start")
                await dp.start_polling(
                    bot,
                    allowed_updates=['message', 'callback_query', 'channel_post']
                )
            
            tasks.append(run_bot())
        
        # Инициализация воркера
        if RUN_WORKER:
            worker = TaskWorker()
            
            async def run_worker():
                logger.info("🔧 Запуск воркера задач...")
                await worker.start()
            
            tasks.append(run_worker())
        
        # Уведомления о готовности
        if RUN_BOT and not RUN_WORKER:
            logger.warning("⚠️ Воркер отключен - задачи просмотра и подписки НЕ будут обрабатываться!")
        elif RUN_WORKER and not RUN_BOT:
            logger.info("ℹ️ Запущен только воркер - бот недоступен для управления")
        else:
            logger.info("✅ Бот и воркер готовы к работе!")
        
        # Запускаем все задачи параллельно
        if len(tasks) == 1:
            await tasks[0]
        else:
            await asyncio.gather(*tasks, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        raise
    finally:
        # Корректное завершение
        try:
            # Останавливаем воркер
            if worker:
                await worker.stop()
            
            # Закрываем бота
            if bot:
                await bot.session.close()
            
            # Закрываем БД
            await shutdown_db_pool()
            
            logger.info("✅ Все компоненты корректно остановлены")
        except Exception as e:
            logger.error(f"Ошибка при остановке: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"💥 Фатальная ошибка: {e}")
