import asyncio
import logging
import time
import json
import random
from typing import Dict, List, Optional
from collections import deque
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
    def __init__(self):
        self.redis_client = None
        self.running = False
        self.max_retries = 3
        self.sessions_loaded = False
        
        self.task_buffer = deque()  # Очередь задач в памяти
        self.max_buffer_size = 3000  # Максимум задач в буфере
        self.min_buffer_size = 2990
        
        # Счетчики производительности
        self.processed_tasks = 0
        self.last_buffer_load = 0
        
    async def start(self):
        logger.info("🚀 Запуск Simple Task Worker...")
        
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
            
            # Загружаем сессии
            logger.info("🧠 Загрузка сессий...")
            await self._try_preload_sessions()
            
            # Запуск основного цикла
            self.running = True
            await self._main_loop()
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка воркера: {e}")
            raise
        finally:
            await self._shutdown()
    
    async def _try_preload_sessions(self):
        """Пытается загрузить сессии"""
        try:
            results = await global_session_manager.preload_all_sessions()
            
            if results['loaded'] == 0:
                logger.warning("⚠️ Нет активных аккаунтов для загрузки")
                self.sessions_loaded = False
            else:
                logger.info(f"✅ Загружено {results['loaded']} сессий")
                self.sessions_loaded = True
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сессий: {e}")
            self.sessions_loaded = False
    
    async def _main_loop(self):
        logger.info("🔄 Запуск цикла обработки задач с буфером")
        
        last_stats_time = time.time()
        last_session_check = time.time()
        last_ban_check = time.time()
        last_buffer_info = time.time()
        
        while self.running:
            try:
                # Проверяем команды от бота
                await self._process_worker_commands()
                
                # Если сессии не загружены, пытаемся загрузить
                if not self.sessions_loaded and time.time() - last_session_check > 300:
                    logger.info("🔍 Проверяю появление новых аккаунтов...")
                    await self._try_preload_sessions()
                    last_session_check = time.time()
                
                # Проверка забаненных аккаунтов (раз в час)
                if time.time() - last_ban_check > 3600:
                    await self._check_banned_accounts_for_retry()
                    last_ban_check = time.time()
                
                if self.sessions_loaded:
                    
                    await self._fill_task_buffer()
        
                    processed = await self._process_buffer_tasks()
                    self.processed_tasks += processed
                    
                    await self._process_retry_tasks()
                
                    if random.random() < 0.002:  # ~0.2% шанс
                        health_stats = await global_session_manager.health_check()
                        if health_stats.get('removed_dead', 0) > 0:
                            logger.info(f"🔧 Очищено {health_stats['removed_dead']} мертвых сессий")
                

                if time.time() - last_buffer_info > 90:
                    await self._log_buffer_info()
                    last_buffer_info = time.time()
                
                # Статистика каждые 5 минут
                if time.time() - last_stats_time > 300:
                    await self._log_performance_stats()
                    self.processed_tasks = 0
                    last_stats_time = time.time()
                
                # Пауза
                sleep_time = 2 if self.sessions_loaded else 10
                await asyncio.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в главном цикле: {e}")
                await asyncio.sleep(5)
    
    async def _fill_task_buffer(self):
        """Загружает готовые задачи из Redis в буфер памяти"""
        try:
            # Проверяем нужно ли загружать
            if len(self.task_buffer) >= self.min_buffer_size:
                return
            
            current_time = time.time()
            
            # Сколько задач нужно загрузить
            needed = self.max_buffer_size - len(self.task_buffer)
            
            # Получаем готовые задачи из Redis
            ready_tasks_data = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max=current_time,
                withscores=True,
                start=0,
                num=needed
            )
            
            if not ready_tasks_data:
                return
            
            loaded_count = 0
            
            for task_json, score in ready_tasks_data:
                try:
                    task_data = json.loads(task_json)
                    task_data['score'] = score  # Сохраняем оригинальный score
                    
                    # Добавляем в буфер
                    self.task_buffer.append(task_data)
                    loaded_count += 1
                    
                    # Удаляем из Redis после загрузки в буфер
                    self.redis_client.zrem("task_queue", task_json)
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки задачи в буфер: {e}")
                    # Удаляем битую задачу
                    self.redis_client.zrem("task_queue", task_json)
            
            if loaded_count > 0:
                self.last_buffer_load = time.time()
                logger.info(f"📥 Загружено {loaded_count} задач в буфер (всего в буфере: {len(self.task_buffer)})")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки задач в буфер: {e}")
    
    async def _process_buffer_tasks(self) -> int:
        """Обрабатывает готовые задачи из буфера"""
        if not self.task_buffer:
            return 0
        
        current_time = time.time()
        processed_count = 0
        
        # Обрабатываем до 50 задач за раз
        max_process = min(50, len(self.task_buffer))
        
        for _ in range(max_process):
            if not self.task_buffer:
                break
            
            # Берем задачу из начала очереди
            task = self.task_buffer.popleft()
            
            # Проверяем время выполнения
            if task.get('execute_at', 0) > current_time:
                self.task_buffer.appendleft(task)
                break
            
            # Выполняем задачу
            try:
                success = await self._execute_task(task)
                if success:
                    processed_count += 1
                
                # Небольшая задержка между задачами
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
            except Exception as e:
                logger.error(f"Ошибка выполнения задачи: {e}")
        
        return processed_count
    
    async def _execute_task(self, task: Dict) -> bool:
        """Выполняет одну задачу (просмотр или подписку)"""
        task_type = task.get('task_type', '')
        
        if task_type == 'view':
            return await self._execute_view_task(task)
        elif task_type == 'subscribe':
            return await self._execute_subscription_task(task)
        else:
            logger.warning(f"Неизвестный тип задачи: {task_type}")
            return False
    
    async def _execute_view_task(self, task: Dict) -> bool:
        """Выполняет задачу просмотра"""
        session_data = task['account_session']
        phone = task['phone']
        channel = task['channel']
        post_id = task.get('post_id', 0)
        
        try:
            # Получаем готовый клиент
            client = global_session_manager.get_client(session_data)
            
            if not client:
                logger.warning(f"❌ {phone}: клиент недоступен")
                await self._handle_task_failure(phone, 'view')
                return False
            
            # Получаем entity канала
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
            await asyncio.sleep(random.uniform(3, 7))
            
            # Успех
            await self._handle_task_success(phone)
            logger.debug(f"✅ {phone}: просмотр поста {post_id} в @{channel}")
            return True
            
        except FloodWaitError as e:
            logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s")
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
    
    async def _execute_subscription_task(self, task: Dict) -> bool:
        """Выполняет задачу подписки"""
        session_data = task['account_session']
        phone = task['phone']
        channel = task['channel']
        
        try:
            # Получаем готовый клиент
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
            
            # Успех
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
                logger.debug(f"🔓 {phone}: восстановлен в active")
        except Exception as e:
            logger.error(f"Ошибка обработки успеха для {phone}: {e}")

    async def _handle_task_failure(self, phone: str, task_type: str):
        """Обрабатывает неудачное выполнение задачи"""
        try:
            fail_count = await increment_account_fails(phone)
            
            if fail_count >= 3:
                await update_account_status(phone, 'ban')
                logger.warning(f"🚫 {phone}: переведен в BAN (неудач: {fail_count})")
                await global_session_manager.remove_session_by_phone(phone)
            else:
                logger.debug(f"⚠️ {phone}: неудача {fail_count}/3 ({task_type})")
                
        except Exception as e:
            logger.error(f"Ошибка обработки неудачи для {phone}: {e}")
    
    async def _add_to_retry_queue(self, task: Dict, task_type: str, delay: int = 0):
        """Добавляет задачу в очередь повторов"""
        try:
            task['retry_count'] = task.get('retry_count', 0) + 1
            task['task_type'] = task_type
            task['retry_after'] = time.time() + delay + random.uniform(60, 300)
            
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
            
            # Обрабатываем до 20 retry задач за раз
            for _ in range(20):
                task_data = self.redis_client.rpop('retry_tasks')
                if not task_data:
                    break
                
                try:
                    task = json.loads(task_data)
                    
                    # Проверяем время повтора
                    if task.get('retry_after', 0) <= current_time:
                        # Время пришло - выполняем
                        success = await self._execute_task(task)
                        if success:
                            logger.debug(f"✅ Retry задача выполнена успешно")
                    else:
                        # Время еще не пришло - возвращаем в очередь
                        self.redis_client.lpush('retry_tasks', task_data)
                        break
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки retry: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Ошибка обработки retry очереди: {e}")
    
    async def _process_worker_commands(self):
        """Обрабатывает команды от бота"""
        try:
            command_data = self.redis_client.rpop('worker_commands')
            if not command_data:
                return
                
            command = json.loads(command_data)
            
            if command['command'] == 'reload_settings':
                logger.info("🔄 Получена команда обновления настроек")
                logger.info("✅ Настройки обновлены")
                
        except Exception as e:
            logger.error(f"Ошибка обработки команд: {e}")
    
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
                    
                    # Создаем тестовую задачу
                    test_task = {
                        'account_session': account['session_data'],
                        'phone': phone,
                        'channel': 'telegram',
                        'lang': account['lang'],
                        'task_type': 'subscribe'
                    }
                    
                    # Пытаемся выполнить тестовую подписку
                    success = await self._execute_task(test_task)
                    
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
    
    async def _log_buffer_info(self):
        """Логирует информацию о буфере задач"""
        try:
            buffer_size = len(self.task_buffer)
            
            # Статистика по типам задач в буфере
            view_count = sum(1 for task in self.task_buffer if task.get('task_type') == 'view')
            sub_count = sum(1 for task in self.task_buffer if task.get('task_type') == 'subscribe')
            
            # Статистика времени
            if self.task_buffer:
                current_time = time.time()
                ready_count = sum(1 for task in self.task_buffer if task.get('execute_at', 0) <= current_time)
                future_count = buffer_size - ready_count
                
                logger.info(f"""
📋 БУФЕР ЗАДАЧ:
   📊 Всего в буфере: {buffer_size}/{self.max_buffer_size}
   👀 Просмотров: {view_count}
   📺 Подписок: {sub_count}
   ✅ Готовых к выполнению: {ready_count}
   ⏳ Будущих: {future_count}
                """)
            else:
                logger.info("📋 БУФЕР ЗАДАЧ: пустой")
            
        except Exception as e:
            logger.error(f"Ошибка логирования буфера: {e}")
    
    async def _log_performance_stats(self):
        """Логирует статистику производительности"""
        try:
            if self.sessions_loaded:
                session_stats = await global_session_manager.get_stats()
                
                # Статистика Redis очереди
                total_in_redis = self.redis_client.zcard("task_queue") or 0
                retry_count = self.redis_client.llen("retry_tasks") or 0
                
                # Производительность
                tasks_per_min = self.processed_tasks / 5 if self.processed_tasks > 0 else 0
                
                # Статус производительности
                if tasks_per_min > 20:
                    performance_status = "🚀 ОТЛИЧНО"
                elif tasks_per_min > 10:
                    performance_status = "✅ ХОРОШО"
                elif tasks_per_min > 5:
                    performance_status = "⚠️ СРЕДНЕ"
                else:
                    performance_status = "❌ НИЗКО"
                
                logger.info(f"""
📊  СТАТИСТИКА (5 мин):
   ✅ Выполнено задач: {self.processed_tasks} ({tasks_per_min:.1f}/мин)
   📋 В буфере: {len(self.task_buffer)}/{self.max_buffer_size}
   📦 В Redis: {total_in_redis}
   🔄 Retry: {retry_count}
   🧠 Сессий: {session_stats['connected']}/{session_stats['total_loaded']}
   🚀 Производительность: {performance_status}
                """)
            else:
                logger.info("⏳ Воркер активен, ожидаю загрузки аккаунтов...")
                
        except Exception as e:
            logger.error(f"Ошибка логирования статистики: {e}")
    
    async def get_buffer_stats(self) -> Dict:
        """Возвращает статистику буфера для внешнего использования"""
        try:
            buffer_size = len(self.task_buffer)
            current_time = time.time()
            
            view_count = sum(1 for task in self.task_buffer if task.get('task_type') == 'view')
            sub_count = sum(1 for task in self.task_buffer if task.get('task_type') == 'subscribe')
            ready_count = sum(1 for task in self.task_buffer if task.get('execute_at', 0) <= current_time)
            
            return {
                'buffer_size': buffer_size,
                'max_buffer_size': self.max_buffer_size,
                'view_tasks': view_count,
                'subscription_tasks': sub_count,
                'ready_tasks': ready_count,
                'future_tasks': buffer_size - ready_count,
                'last_buffer_load': self.last_buffer_load
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики буфера: {e}")
            return {}
    
    async def reload_sessions(self) -> bool:
        """Принудительная перезагрузка сессий"""
        logger.info("🔄 Принудительная перезагрузка сессий...")
        try:
            await global_session_manager.shutdown()
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
    
    async def clear_task_buffer(self):
        """Очищает буфер задач (для экстренных случаев)"""
        logger.info("🗑️ Очистка буфера задач...")
        cleared_count = len(self.task_buffer)
        self.task_buffer.clear()
        logger.info(f"✅ Очищено {cleared_count} задач из буфера")
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка  воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы  воркера...")
        
        try:
            # Сохраняем задачи из буфера обратно в Redis перед завершением
            if self.task_buffer:
                logger.info(f"💾 Сохранение {len(self.task_buffer)} задач из буфера в Redis...")
                
                tasks_data = {}
                for task in self.task_buffer:
                    try:
                        task_json = json.dumps(task)
                        execute_at = task.get('execute_at', time.time())
                        tasks_data[task_json] = execute_at
                    except Exception as e:
                        logger.error(f"Ошибка сохранения задачи: {e}")
                
                if tasks_data:
                    self.redis_client.zadd("task_queue", tasks_data)
                    logger.info(f"✅ Сохранено {len(tasks_data)} задач в Redis")
            
            # Закрываем все сессии
            await global_session_manager.shutdown()
            
            # Закрываем Redis
            if self.redis_client:
                self.redis_client.close()
            
            logger.info("✅ Воркер корректно завершен")
            
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
    
    # Запускаем  воркер
    asyncio.run(main())