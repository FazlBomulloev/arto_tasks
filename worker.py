import asyncio
import logging
import time
import json
import random
from typing import Dict, List, Optional
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError, AuthKeyInvalidError
from telethon.tl.functions.messages import GetMessagesViewsRequest
from telethon.tl.functions.channels import JoinChannelRequest

from config import find_lang_code, API_ID, API_HASH, read_setting
from database import (
    init_db_pool, shutdown_db_pool, update_account_status,
    increment_account_fails, reset_account_fails, 
    get_ban_accounts_for_retry, mark_account_retry_attempt
)
from session_manager import global_session_manager
from exceptions import SessionError, RateLimitError
from redis import Redis
from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

# Настройка логгера для воркера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [WORKER] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/worker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('worker')

class TaskWorker:
    """Воркер для обработки задач просмотров и подписок"""
    
    def __init__(self):
        self.redis_client = None
        self.running = False
        self.max_retries = 3
        self.sessions_loaded = False
        self.ban_retry_hours = 120  # 120 часов между попытками для забаненных
        
    async def start(self):
        """Запуск воркера"""
        logger.info("🚀 Запуск Task Worker...")
        
        try:
            # Инициализация БД
            await init_db_pool()
            logger.info("✅ База данных подключена")
            
            # Инициализация Redis
            self.redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            logger.info("✅ Redis подключен")
            
            # Пытаемся загрузить сессии, но НЕ останавливаемся при их отсутствии
            logger.info("🧠 Попытка предзагрузки сессий...")
            await self._try_preload_sessions()
            
            # Запуск основного цикла (работает даже без сессий)
            self.running = True
            await self._main_loop()
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка воркера: {e}")
            raise
        finally:
            await self._shutdown()
    
    async def _try_preload_sessions(self):
        """Пытается загрузить сессии, но не останавливает воркер при их отсутствии"""
        try:
            results = await global_session_manager.preload_all_sessions()
            
            if results['loaded'] == 0:
                logger.warning("⚠️ Нет активных аккаунтов для загрузки")
                logger.info("💡 Воркер будет ждать появления аккаунтов...")
                self.sessions_loaded = False
            else:
                logger.info(f"✅ Предзагрузка завершена: {results['loaded']} сессий готово к работе")
                self.sessions_loaded = True
                
            # ВАЖНО: Обновляем статус из session_manager
            self.sessions_loaded = global_session_manager.loading_complete
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке сессий: {e}")
            logger.info("💡 Воркер продолжит работу в режиме ожидания аккаунтов...")
            self.sessions_loaded = False
    
    async def _main_loop(self):
        """Основной цикл обработки задач с УМНОЙ проверкой сессий"""
        logger.info("🔄 Запуск основного цикла обработки задач")
        
        # Счетчики для статистики
        processed_views = 0
        processed_subs = 0
        last_stats_time = time.time()
        last_session_check = time.time()
        last_ban_check = time.time()
        last_health_check = time.time()
        
        while self.running:
            try:
                # УМНАЯ проверка сессий - только если действительно нужно
                current_time = time.time()
                
                # 1. Если сессии НЕ загружены, проверяем каждые 5 минут
                if not self.sessions_loaded and current_time - last_session_check > 300:
                    logger.info("🔍 Проверяю появление новых аккаунтов...")
                    await self._try_preload_sessions()
                    last_session_check = current_time
                
                # 2. Если сессии загружены, но session_manager говорит что нет - синхронизируем
                elif self.sessions_loaded and not global_session_manager.loading_complete:
                    logger.warning("⚠️ Рассинхронизация статуса сессий, исправляю...")
                    self.sessions_loaded = global_session_manager.loading_complete
                    
                # 3. Если долго нет новых аккаунтов, проверяем раз в час
                elif self.sessions_loaded and len(global_session_manager.clients) == 0 and current_time - last_session_check > 3600:
                    logger.info("🔍 Периодическая проверка новых аккаунтов (раз в час)...")
                    await self._try_preload_sessions()
                    last_session_check = current_time
                
                # Проверка забаненных аккаунтов (раз в час)
                if current_time - last_ban_check > 3600:
                    await self._check_banned_accounts_for_retry()
                    last_ban_check = current_time
                
                # Основная работа - только если сессии загружены
                if self.sessions_loaded and len(global_session_manager.clients) > 0:
                    # Обрабатываем отложенные просмотры
                    view_tasks = await self._get_ready_view_tasks()
                    if view_tasks:
                        await self._process_view_batch(view_tasks)
                        processed_views += len(view_tasks)
                    
                    # Обрабатываем подписки
                    sub_tasks = await self._get_ready_subscription_tasks()
                    if sub_tasks:
                        await self._process_subscription_batch(sub_tasks)
                        processed_subs += len(sub_tasks)
                    
                    # Обрабатываем retry задачи
                    await self._process_retry_tasks()
                    
                    # Health check сессий каждые 30 минут (было 15)
                    if current_time - last_health_check > 1800:
                        health_stats = await global_session_manager.health_check()
                        if health_stats.get('removed_dead', 0) > 0:
                            logger.info(f"🔧 Очищено {health_stats['removed_dead']} мертвых сессий")
                        last_health_check = current_time
                
                # Статистика каждые 5 минут
                if current_time - last_stats_time > 300:
                    if self.sessions_loaded and len(global_session_manager.clients) > 0:
                        session_stats = await global_session_manager.get_stats()
                        logger.info(f"""
📊 Статистика за 5 минут:
   👀 Просмотров: {processed_views}
   📺 Подписок: {processed_subs}
   🧠 Сессий активно: {session_stats['connected']}/{session_stats['total_loaded']}
                        """)
                    elif self.sessions_loaded:
                        logger.info("⏳ Сессии загружены, но аккаунтов нет в пуле")
                    else:
                        logger.info("⏳ Воркер активен, ожидаю загрузки аккаунтов...")
                    
                    processed_views = processed_subs = 0
                    last_stats_time = current_time
                
                # Пауза между циклами
                if self.sessions_loaded and len(global_session_manager.clients) > 0:
                    sleep_time = 5  # Активная работа - короткие паузы
                else:
                    sleep_time = 30  # Ожидание - длинные паузы
                    
                await asyncio.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в основном цикле: {e}")
                await asyncio.sleep(10)  # Пауза при ошибке
    
    async def _check_banned_accounts_for_retry(self):
        """Проверяет забаненные аккаунты раз в 120 часов"""
        try:
            ban_accounts = await get_ban_accounts_for_retry()
            
            if not ban_accounts:
                return
            
            logger.info(f"🔍 Проверяю {len(ban_accounts)} забаненных аккаунтов...")
            
            for account in ban_accounts:
                phone = account['phone_number']
                
                try:
                    # Отмечаем попытку проверки
                    await mark_account_retry_attempt(phone)
                    
                    # Создаем простую тестовую задачу
                    test_task = {
                        'account_session': account['session_data'],
                        'phone': phone,
                        'channel': 'telegram',  # Простой канал для теста
                        'lang': account['lang']
                    }
                    
                    # Пытаемся выполнить тестовую подписку
                    success = await self._execute_subscription_task(test_task)
                    
                    if success:
                        logger.info(f"🔓 {phone}: восстановлен из бана!")
                    else:
                        logger.info(f"🚫 {phone}: остается в бане")
                        
                    # Задержка между проверками
                    await asyncio.sleep(random.uniform(30, 60))
                    
                except Exception as e:
                    logger.error(f"Ошибка проверки забаненного аккаунта {phone}: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка проверки забаненных аккаунтов: {e}")
    
    async def _get_ready_view_tasks(self) -> List[Dict]:
        """Получает готовые к выполнению задачи просмотра"""
        if not self.sessions_loaded:
            return []
            
        try:
            current_time = time.time()
            ready_tasks = []
            
            # Получаем батчи из очереди
            while True:
                batch_data = self.redis_client.rpop('delayed_view_batches')
                if not batch_data:
                    break
                
                try:
                    batch = json.loads(batch_data)
                    
                    # Проверяем время выполнения
                    if batch['execute_at'] <= current_time:
                        # Время пришло - добавляем задачи
                        ready_tasks.extend(batch['tasks'])
                        logger.debug(f"⏰ Батч #{batch['batch_number']} готов: {len(batch['tasks'])} задач")
                    else:
                        # Время еще не пришло - возвращаем в очередь
                        self.redis_client.lpush('delayed_view_batches', batch_data)
                        break
                        
                except Exception as e:
                    logger.error(f"Ошибка парсинга батча: {e}")
                    continue
            
            return ready_tasks[:1000]  # Максимум 1000 задач за раз
            
        except Exception as e:
            logger.error(f"Ошибка получения задач просмотра: {e}")
            return []
    
    async def _get_ready_subscription_tasks(self) -> List[Dict]:
        """Получает готовые к выполнению задачи подписки С ПРОВЕРКОЙ ВРЕМЕНИ"""
        if not self.sessions_loaded:
            return []
            
        try:
            current_time = time.time()
            ready_tasks = []
            checked_tasks = 0
            max_checks = 200  # Максимум задач для проверки за раз
            
            # Получаем задачи из очереди и проверяем время
            temp_storage = []  # Временное хранение "не готовых" задач
            
            while checked_tasks < max_checks:
                task_data = self.redis_client.rpop('subscription_tasks')
                if not task_data:
                    break
                
                checked_tasks += 1
                
                try:
                    task = json.loads(task_data)
                    
                    # Проверяем время выполнения
                    execute_at = task.get('execute_at', 0)
                    
                    if execute_at <= current_time:
                        # Время пришло - добавляем к готовым
                        ready_tasks.append(task)
                        phone = task.get('phone', 'unknown')
                        delay_was = (current_time - task.get('created_at', current_time)) / 60
                        logger.info(f"⏰ Подписка готова: @{phone} (ждала {delay_was:.1f} мин)")
                    else:
                        # Время еще не пришло - сохраняем для возврата
                        temp_storage.append(task_data)
                        
                except Exception as e:
                    logger.error(f"Ошибка парсинга задачи подписки: {e}")
                    continue
            
            # Возвращаем "не готовые" задачи обратно в очередь
            for task_data in temp_storage:
                self.redis_client.lpush('subscription_tasks', task_data)
            
            if ready_tasks:
                logger.info(f"📺 Готово {len(ready_tasks)} подписок из {checked_tasks} проверенных")
            
            return ready_tasks[:50]  # Максимум 50 подписок за раз
            
        except Exception as e:
            logger.error(f"Ошибка получения задач подписки: {e}")
            return []
    
    async def _process_view_batch(self, tasks: List[Dict]):
        """Обрабатывает батч задач просмотра (теперь обычно 1 задача)"""
        if not tasks:
            return
        
        task_count = len(tasks)
        
        if task_count == 1:
            # Одиночная задача (обычный случай)
            task = tasks[0]
            phone = task.get('phone', 'unknown')
            logger.info(f"👀 Обрабатываю просмотр для @{phone}")
            
            success = await self._execute_view_task(task)
            if success:
                logger.info(f"✅ @{phone}: просмотр выполнен успешно")
            else:
                logger.warning(f"❌ @{phone}: просмотр не выполнен")
                
        else:
            # Множественные задачи (старый формат батчей)
            logger.info(f"👀 Обрабатываю {task_count} задач просмотра")
            
            success_count = 0
            for idx, task in enumerate(tasks):
                try:
                    result = await self._execute_view_task(task)
                    if result:
                        success_count += 1
                    
                    # Задержка между задачами в батче (если их несколько)
                    if idx < len(tasks) - 1:
                        delay = random.uniform(30, 60)
                        logger.info(f"⏳ Пауза {delay:.1f}с между задачами в батче...")
                        await asyncio.sleep(delay)
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки просмотра: {e}")
            
            logger.info(f"✅ Просмотры в батче: {success_count}/{task_count} успешно")
    
    async def _execute_view_task(self, task: Dict) -> bool:
        """Выполняет одну задачу просмотра с обработкой банов"""
        session_data = task['account_session']
        phone = task['phone']
        channel = task['channel']
        post_id = task['post_id']
        
        try:
            # Получаем готовый клиент из пула
            client = global_session_manager.get_client(session_data)
            
            if not client:
                logger.warning(f"❌ {phone}: клиент недоступен")
                await self._handle_task_failure(phone, 'view')
                return False
            
            # Добавляем небольшую случайную задержку внутри батча
            await asyncio.sleep(random.uniform(0.1, 2.0))
            
            # Получаем entity канала (можно кэшировать)
            try:
                channel_entity = await client.get_entity(channel)
            except Exception as e:
                logger.warning(f"❌ {phone}: не удалось получить канал @{channel}: {e}")
                await self._handle_task_failure(phone, 'view')
                return False
            
            # Выполняем просмотр
            await client(GetMessagesViewsRequest(
                peer=channel_entity,
                id=[post_id],
                increment=True
            ))
            
            # Имитируем время чтения
            await asyncio.sleep(random.uniform(1, 3))
            
            # УСПЕХ - сбрасываем счетчик неудач
            await self._handle_task_success(phone)
            logger.debug(f"✅ {phone}: просмотр поста {post_id} в @{channel}")
            return True
            
        except FloodWaitError as e:
            logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s")
            # Добавляем в retry с задержкой
            await self._add_to_retry_queue(task, 'view', delay=e.seconds)
            return False
            
        except (RPCError, AuthKeyInvalidError) as e:
            logger.warning(f"❌ {phone}: критическая ошибка - {e}")
            await self._handle_task_failure(phone, 'view')
            return False
            
        except Exception as e:
            logger.error(f"💥 {phone}: неожиданная ошибка - {e}")
            await self._handle_task_failure(phone, 'view')
            return False
    
    async def _process_subscription_batch(self, tasks: List[Dict]):
        """Обрабатывает батч задач подписки с ДОПОЛНИТЕЛЬНЫМИ задержками"""
        if not tasks:
            return
        
        logger.info(f"📺 Обрабатываю {len(tasks)} задач подписки")
        
        success_count = 0
        
        for idx, task in enumerate(tasks):
            try:
                success = await self._execute_subscription_task(task)
                if success:
                    success_count += 1
                
                # УВЕЛИЧЕННЫЕ задержки между подписками в батче
                if idx < len(tasks) - 1:  # Не ждем после последней
                    # Читаем актуальную задержку между аккаунтами
                    accounts_delay_minutes = read_setting('accounts_delay.txt', 10.0)
                    
                    # Применяем задержку с рандомизацией
                    delay = accounts_delay_minutes * 60 * random.uniform(0.8, 1.2)
                    
                    logger.info(f"⏳ Пауза {delay/60:.1f} мин перед следующей подпиской...")
                    await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Ошибка обработки подписки: {e}")
        
        logger.info(f"✅ Подписки в батче: {success_count}/{len(tasks)} успешно")
        
        # Дополнительная пауза после батча (читаем из настроек)
        batch_pause_minutes = read_setting('timeout_duration.txt', 20.0)
        batch_pause = batch_pause_minutes * 60
        
        logger.info(f"⏳ Пауза {batch_pause_minutes} мин после батча подписок...")
        await asyncio.sleep(batch_pause)
    
    async def _execute_subscription_task(self, task: Dict) -> bool:
        """Выполняет одну задачу подписки с обработкой банов"""
        session_data = task['account_session']
        phone = task['phone']
        channel = task['channel']
        
        try:
            # Получаем готовый клиент из пула
            client = global_session_manager.get_client(session_data)
            
            if not client:
                logger.warning(f"❌ {phone}: клиент недоступен для подписки")
                await self._handle_task_failure(phone, 'subscription')
                return False
            
            # Получаем entity канала
            try:
                channel_entity = await client.get_entity(channel)
            except Exception as e:
                logger.warning(f"❌ {phone}: не удалось получить канал @{channel}: {e}")
                await self._handle_task_failure(phone, 'subscription')
                return False
            
            # Выполняем подписку
            await client(JoinChannelRequest(channel_entity))
            
            # УСПЕХ - сбрасываем счетчик неудач
            await self._handle_task_success(phone)
            logger.info(f"✅ {phone}: подписан на @{channel}")
            return True
            
        except FloodWaitError as e:
            logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s при подписке")
            await self._add_to_retry_queue(task, 'subscription', delay=e.seconds)
            return False
            
        except (RPCError, AuthKeyInvalidError) as e:
            logger.warning(f"❌ {phone}: критическая ошибка подписки - {e}")
            await self._handle_task_failure(phone, 'subscription')
            return False
            
        except Exception as e:
            logger.error(f"💥 {phone}: неожиданная ошибка подписки - {e}")
            await self._handle_task_failure(phone, 'subscription')
            return False
    
    async def _handle_task_success(self, phone: str):
        """Обрабатывает успешное выполнение задачи"""
        try:
            success = await reset_account_fails(phone)
            if success:
                logger.info(f"🔓 {phone}: восстановлен в active (счетчик неудач сброшен)")
        except Exception as e:
            logger.error(f"Ошибка обработки успеха для {phone}: {e}")

    async def _handle_task_failure(self, phone: str, task_type: str):
        """Обрабатывает неудачное выполнение задачи"""
        try:
            fail_count = await increment_account_fails(phone)
            
            if fail_count >= 3:
                # Переводим в бан после 3 неудач
                await update_account_status(phone, 'ban')
                logger.warning(f"🚫 {phone}: переведен в BAN (неудач: {fail_count})")
                
                # Удаляем из пула сессий
                await global_session_manager.remove_session_by_phone(phone)
            else:
                logger.info(f"⚠️ {phone}: неудача {fail_count}/3 ({task_type})")
                
        except Exception as e:
            logger.error(f"Ошибка обработки неудачи для {phone}: {e}")
    
    async def _add_to_retry_queue(self, task: Dict, task_type: str, delay: int = 0):
        """Добавляет задачу в очередь повторов"""
        try:
            task['retry_count'] = task.get('retry_count', 0) + 1
            task['task_type'] = task_type
            task['retry_after'] = time.time() + delay + random.uniform(60, 300)  # Дополнительная задержка
            
            if task['retry_count'] <= self.max_retries:
                self.redis_client.lpush('retry_tasks', json.dumps(task))
                logger.debug(f"🔄 Задача добавлена в retry (попытка {task['retry_count']}/{self.max_retries})")
            else:
                logger.warning(f"❌ Задача отброшена после {self.max_retries} попыток")
                
        except Exception as e:
            logger.error(f"Ошибка добавления в retry: {e}")
    
    async def _process_retry_tasks(self):
        """Обрабатывает задачи из очереди повторов"""
        try:
            current_time = time.time()
            
            for _ in range(50):  # Максимум 50 retry задач за раз
                task_data = self.redis_client.rpop('retry_tasks')
                if not task_data:
                    break
                
                try:
                    task = json.loads(task_data)
                    
                    # Проверяем время повтора
                    if task.get('retry_after', 0) <= current_time:
                        # Время пришло - выполняем
                        if task['task_type'] == 'view':
                            await self._execute_view_task(task)
                        elif task['task_type'] == 'subscription':
                            await self._execute_subscription_task(task)
                    else:
                        # Время еще не пришло - возвращаем в очередь
                        self.redis_client.lpush('retry_tasks', task_data)
                        break
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки retry: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Ошибка обработки retry очереди: {e}")
    
    async def reload_sessions(self) -> bool:
        """Принудительная перезагрузка сессий (для вызова из бота)"""
        logger.info("🔄 Принудительная перезагрузка сессий...")
        try:
            # Закрываем старые сессии
            await global_session_manager.shutdown()
            
            # Загружаем новые
            await self._try_preload_sessions()
            
            if self.sessions_loaded:
                logger.info("✅ Сессии успешно перезагружены")
                return True
            else:
                logger.warning("⚠️ Сессии не загружены после перезагрузки")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка перезагрузки сессий: {e}")
            return False
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы воркера...")
        
        try:
            # Закрываем все сессии
            await global_session_manager.shutdown()
            
            # Закрываем Redis
            if self.redis_client:
                self.redis_client.close()
            
            logger.info("✅ Воркер корректно завершен (БД остается активная)")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении: {e}")

# Запуск воркера
async def main():
    """Главная функция воркера"""
    worker = TaskWorker()
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("⏹️ Получен Ctrl+C, завершаем работу...")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        await worker.stop()

if __name__ == "__main__":
    # Устанавливаем uvloop если доступен
    try:
        import uvloop
        uvloop.install()
        logger.info("✅ uvloop установлен")
    except ImportError:
        logger.info("⚠️ uvloop недоступен, используем стандартный event loop")
    
    # Запускаем воркер
    asyncio.run(main())