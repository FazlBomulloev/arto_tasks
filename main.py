import asyncio
import logging
import logging.config
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from pathlib import Path

from config import BOT_TOKEN, LOGGING_CONFIG, RUN_WORKER, RUN_BOT, VARS_DIR
from database import init_db_pool, create_tables, shutdown_db_pool
from handlers import get_all_routers
from worker import SimpleTaskWorker

# Настройка логирования
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

class BotManager:
    def __init__(self):
        self.bot = None
        self.dp = None
        self.last_update_id = None
    
    async def get_last_update_id(self):
        """Получает ID последнего обновления для пропуска старых сообщений"""
        try:
            # Получаем последние обновления без их обработки
            updates = await self.bot.get_updates(limit=1, timeout=1)
            if updates:
                self.last_update_id = updates[-1].update_id
                logger.info(f"📝 Последнее обновление ID: {self.last_update_id}")
            else:
                logger.info("📝 Нет предыдущих обновлений")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить последние обновления: {e}")
    
    async def skip_pending_updates(self):
        """Пропускает все накопившиеся обновления"""
        try:
            logger.info("🔄 Пропускаю все старые обновления...")
            
            # Получаем все накопившиеся обновления и пропускаем их
            skipped_count = 0
            while True:
                updates = await self.bot.get_updates(
                    offset=self.last_update_id + 1 if self.last_update_id else None,
                    limit=100,
                    timeout=2
                )
                
                if not updates:
                    break
                
                # Обновляем offset для следующей итерации
                self.last_update_id = updates[-1].update_id
                skipped_count += len(updates)
                
                # Если получили меньше 100 обновлений, значит больше нет
                if len(updates) < 100:
                    break
            
            if skipped_count > 0:
                logger.info(f"⏭️ Пропущено {skipped_count} старых обновлений")
                
                # Подтверждаем получение всех старых обновлений
                await self.bot.get_updates(
                    offset=self.last_update_id + 1,
                    limit=1,
                    timeout=1
                )
            
        except Exception as e:
            logger.error(f"❌ Ошибка пропуска старых обновлений: {e}")
    
    async def setup_bot(self):
        """Настройка бота"""
        try:
            # Создаем бота с настройками по умолчанию
            self.bot = Bot(
                token=BOT_TOKEN,
                default=DefaultBotProperties(
                    parse_mode=ParseMode.HTML
                )
            )
            
            # Создаем диспетчер
            self.dp = Dispatcher()
            
            # Подключение роутеров
            routers = get_all_routers()
            self.dp.include_routers(*routers)
            logger.info(f"✅ Подключено {len(routers)} роутеров")
            
            # Получаем информацию о боте
            bot_info = await self.bot.get_me()
            logger.info(f"🤖 Бот запущен: @{bot_info.username} ({bot_info.full_name})")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка настройки бота: {e}")
            return False
    
    async def start_polling(self):
        """Запуск polling с правильной обработкой обновлений"""
        try:
            logger.info("🚀 Запуск Telegram бота...")
            logger.info("📝 Доступные команды: /start")
            logger.info("📺 Отслеживание: только НОВЫЕ посты в каналах")
            
            # Пропускаем все старые обновления
            await self.skip_pending_updates()
            
            # Запускаем polling с правильными параметрами
            await self.dp.start_polling(
                self.bot,
                # Обрабатываем только нужные типы обновлений
                allowed_updates=[
                    'message',           # Сообщения в личке
                    'callback_query',    # Нажатия на кнопки
                    'channel_post'       # ТОЛЬКО новые посты в каналах
                ],
                # Начинаем с чистого листа
                offset=self.last_update_id + 1 if self.last_update_id else None,
                # Настройки polling
                timeout=10,
                drop_pending_updates=False  # Мы уже сами обработали старые
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка polling: {e}")
            raise

async def main():
    if not RUN_BOT and not RUN_WORKER:
        logger.error("❌ И бот, и воркер отключены в конфигурации!")
        return
    
    mode_description = []
    if RUN_BOT:
        mode_description.append("🤖 Бот")
    if RUN_WORKER:
        mode_description.append("🔧 Воркер")
    
    logger.info(f"🚀 Запуск: {' + '.join(mode_description)} (ТОЛЬКО новые посты)")
    
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
        bot_manager = None
        worker = None
        
        # Инициализация бота
        if RUN_BOT:
            bot_manager = BotManager()
            
            # Настраиваем бота
            setup_success = await bot_manager.setup_bot()
            if not setup_success:
                logger.error("❌ Не удалось настроить бота")
                return
            
            async def run_bot():
                await bot_manager.start_polling()
            
            tasks.append(run_bot())
        
        # Инициализация воркера
        if RUN_WORKER:
            worker = SimpleTaskWorker()
            
            async def run_worker():
                await worker.start()
            
            tasks.append(run_worker())
        
        # Уведомления о готовности
        if RUN_BOT and not RUN_WORKER:
            logger.warning("⚠️ Воркер отключен - задачи просмотра и подписки НЕ будут обрабатываться!")
        elif RUN_WORKER and not RUN_BOT:
            logger.info("ℹ️ Запущен только воркер - бот недоступен для управления")
        else:
            logger.info("✅ Бот и воркер готовы к работе!")
            logger.info("📺 Бот будет обрабатывать ТОЛЬКО новые посты в каналах")
        
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
            if bot_manager and bot_manager.bot:
                await bot_manager.bot.session.close()
            
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