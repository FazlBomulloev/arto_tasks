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



async def main():
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
