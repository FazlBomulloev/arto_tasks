import asyncio
import logging
import time
import json
import random
from typing import Dict, List, Optional
from collections import deque
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError, AuthKeyInvalidError
from telethon.tl.functions.messages import GetMessagesViewsRequest
from telethon.tl.functions.channels import JoinChannelRequest

from config import find_lang_code, API_ID, API_HASH, read_setting, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from database import (
    init_db_pool, shutdown_db_pool, update_account_status,
    increment_account_fails, reset_account_fails, 
    get_ban_accounts_for_retry, mark_account_retry_attempt
)
from exceptions import SessionError, RateLimitError
from redis import Redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [WORKER] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/worker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('worker')

class MixedBatchWorker:
    def __init__(self):
        self.redis_client = None
        self.running = False
        self.max_retries = 3
        self.restart_count = 0
        self.max_restarts = 10
        
        # Кэш настроек
        self.cached_settings = {}
        self.last_settings_update = 0
        
        # УПРОЩЕННЫЕ счетчики статистики согласно требованиям
        current_time = time.time()
        self.performance_stats = {
            'start_time': current_time,
            
            # Счетчики для расчета среднего за секунду (последние 60 минут)
            'tasks_last_minute': deque(maxlen=60),    # За каждую минуту (60 минут)
            
            # Счетчики для расчета времени выполнения (последние 24 часа) 
            'tasks_last_24h': deque(maxlen=1440),     # За каждую минуту (24 часа = 1440 минут)
            
            'last_minute_update': current_time,
            'tasks_current_minute': 0,
            'last_stats_save': current_time
        }
        
    async def start(self):
        """Запуск воркера с автоматическим восстановлением"""
        logger.info("🚀 Запуск Mixed Batch Worker (просмотры + подписки в одном батче)...")
        
        while self.restart_count < self.max_restarts:
            try:
                await self._initialize_connections()
                await self._run_main_cycle()
                break
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                break
                
            except Exception as e:
                self.restart_count += 1
                logger.error(f"💥 КРИТИЧЕСКИЙ СБОЙ #{self.restart_count}: {e}")
                
                if self.restart_count >= self.max_restarts:
                    logger.error(f"❌ Превышен лимит перезапусков ({self.max_restarts})")
                    break
                
                await self._handle_crash_recovery()
        
        await self._shutdown()
    
    async def _initialize_connections(self):
        """Инициализация подключений"""
        try:
            await init_db_pool()
            logger.info("✅ База данных подключена")
            
            if self.redis_client:
                try:
                    self.redis_client.close()
                except:
                    pass
                    
            self.redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            self.redis_client.ping()
            logger.info("✅ Redis подключен")
            
            await self._update_cached_settings()
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    async def _handle_crash_recovery(self):
        """Обработка восстановления после сбоя"""
        logger.warning(f"🔄 ВОССТАНОВЛЕНИЕ ПОСЛЕ СБОЯ #{self.restart_count}")
        
        try:
            await self._cleanup_connections()
            await self._clear_ready_tasks()
            
            logger.info("⏳ Пауза 2 минуты для стабилизации системы...")
            await asyncio.sleep(120)
            
            self._reset_worker_state()
            logger.info(f"🔄 Готов к перезапуску #{self.restart_count + 1}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка восстановления: {e}")
            await asyncio.sleep(60)
    
    async def _cleanup_connections(self):
        """Очищает старые соединения"""
        try:
            if self.redis_client:
                self.redis_client.close()
                self.redis_client = None
        except Exception as e:
            logger.warning(f"⚠️ Ошибка очистки соединений: {e}")
    
    async def _clear_ready_tasks(self):
        """Удаляет готовые задачи из Redis (избегаем дублей при перезапуске)"""
        try:
            if not self.redis_client:
                return
                
            current_time = time.time()
            ready_tasks = self.redis_client.zrangebyscore(
                "task_queue", min=0, max=current_time, start=0, num=10000
            )
            
            if ready_tasks:
                for task_json in ready_tasks:
                    self.redis_client.zrem("task_queue", task_json)
                
                logger.warning(f"🗑️ УДАЛЕНО {len(ready_tasks)} готовых задач после сбоя")
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки готовых задач: {e}")
    
    def _reset_worker_state(self):
        """Сбрасывает внутреннее состояние воркера"""
        self.running = False
        current_time = time.time()
        self.performance_stats['last_minute_update'] = current_time
        self.performance_stats['last_stats_save'] = current_time
        self.performance_stats['tasks_current_minute'] = 0
        logger.info("🔄 Состояние воркера сброшено")
    
    async def _update_cached_settings(self):
        """Обновляет кэшированные настройки для смешанных батчей"""
        try:
            self.cached_settings = {
                # НОВЫЕ параметры просмотров  
                'view_reading_time': read_setting('view_reading_time.txt', 5.0),         # X2
                'view_connection_pause': read_setting('view_connection_pause.txt', 3.0), # X1
                
                # НОВЫЕ параметры смешанных батчей
                'mixed_batch_size': int(read_setting('mixed_batch_size.txt', 500.0)),
                'mixed_batch_pause': read_setting('mixed_batch_pause.txt', 30.0),
                
                # Старые параметры
                'view_period': read_setting('followPeriod.txt', 3.0) * 3600,
                'sub_lag': read_setting('lag.txt', 14.0) * 60,
                'sub_range': read_setting('range.txt', 5.0) * 60,
                'timeout_count': int(read_setting('timeout_count.txt', 3.0)),
                'timeout_duration': read_setting('timeout_duration.txt', 13.0) * 60,
                'accounts_delay': read_setting('accounts_delay.txt', 2.0) * 60
            }
            
            self.last_settings_update = time.time()
            
            logger.info(f"""
⚙️ НАСТРОЙКИ MIXED BATCH ВОРКЕРА ОБНОВЛЕНЫ:
   📦 СМЕШАННЫЕ БАТЧИ:
   📊 Размер батча: {self.cached_settings['mixed_batch_size']} задач (любых типов)
   ⏸️ Пауза между батчами: {self.cached_settings['mixed_batch_pause']} сек
   
   👀 ПАРАМЕТРЫ ПРОСМОТРОВ:
   📖 Время просмотра: {self.cached_settings['view_reading_time']} сек (X2)
   🔌 Пауза подключ/выкл: {self.cached_settings['view_connection_pause']} сек (X1)
   
   📺 ПОДПИСКИ:
   📅 Базовая задержка: {self.cached_settings['sub_lag']/60:.1f} мин
   🎲 Разброс: {self.cached_settings['sub_range']/60:.1f} мин
            """)
            
        except Exception as e:
            logger.error(f"Ошибка обновления настроек: {e}")
    
    async def _run_main_cycle(self):
        """Запуск основного цикла с смешанными батчами"""
        logger.info("🔄 Запуск основного цикла с СМЕШАННЫМИ батчами")
        
        self.running = True
        last_ban_check = time.time()
        last_cleanup = time.time()
        last_stats_save = time.time()
        cycle_count = 0
        
        if cycle_count == 0:
            self.restart_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                current_time = time.time()
                
                await self._process_worker_commands()
                
                if current_time - self.last_settings_update > 300:
                    await self._update_cached_settings()
                
                if current_time - last_stats_save > 60:
                    await self._save_simplified_stats_to_redis()
                    last_stats_save = current_time
                
                if current_time - last_ban_check > 3600:
                    await self._check_banned_accounts_for_retry()
                    last_ban_check = current_time
                
                if current_time - last_cleanup > 21600:
                    await self._cleanup_old_tasks()
                    last_cleanup = current_time
                
                # ГЛАВНАЯ ЛОГИКА - обработка смешанных батчей
                processed_in_cycle = await self._process_mixed_batch()
                
                await self._process_retry_tasks()
                
                if cycle_count % 100 == 0:
                    await self._log_simple_stats()
                
                # Адаптивная пауза
                if processed_in_cycle > 0:
                    pause_time = random.uniform(10, 20)
                else:
                    pause_time = random.uniform(30, 60)
                    
                if cycle_count % 50 == 0:
                    queue_size = self.redis_client.zcard("task_queue") or 0
                    ready_count = self.redis_client.zcount("task_queue", 0, current_time) or 0
                    logger.info(f"💓 Цикл #{cycle_count} | Очередь: {queue_size} | Готовых: {ready_count}")
                    
                await asyncio.sleep(pause_time)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                self.running = False
                break
                
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле #{cycle_count}: {e}")
                await asyncio.sleep(30)
                
                if cycle_count > 0 and cycle_count % 10 == 0:
                    logger.warning("🔄 Слишком много ошибок, инициирую перезапуск")
                    raise Exception("Too many consecutive errors")
    
    async def _process_mixed_batch(self) -> int:
        """
        ГЛАВНАЯ ФУНКЦИЯ: Обрабатывает смешанный батч (просмотры + подписки вместе)
        """
        current_time = time.time()
        
        try:
            # Получаем готовые задачи ЛЮБЫХ типов до лимита батча
            mixed_tasks = await self._get_ready_mixed_tasks(current_time)
            
            if not mixed_tasks:
                return 0
            
            # Анализируем содержимое батча
            view_count = sum(1 for task in mixed_tasks if task.get('task_type') == 'view')
            subscribe_count = sum(1 for task in mixed_tasks if task.get('task_type') == 'subscribe')
            total_count = len(mixed_tasks)
            
            batch_start_time = time.time()
            logger.info(f"""
📦 ОБРАБАТЫВАЮ СМЕШАННЫЙ БАТЧ:
   📊 Всего задач: {total_count}
   👀 Просмотров: {view_count}
   📺 Подписок: {subscribe_count}
            """)
            
            # Выполняем ВСЕ задачи параллельно (смешанно)
            results = await self._execute_mixed_tasks_parallel(mixed_tasks)
            
            batch_duration = time.time() - batch_start_time
            success_count = sum(1 for r in results if r is True)
            success_rate = (success_count / total_count) * 100 if total_count > 0 else 0
            
            logger.info(f"""
✅ СМЕШАННЫЙ БАТЧ ЗАВЕРШЕН:
   ⏱️ Время выполнения: {batch_duration:.1f}с
   📊 Успешность: {success_count}/{total_count} ({success_rate:.1f}%)
            """)
            
            # Пауза между смешанными батчами
            batch_pause = self.cached_settings['mixed_batch_pause']
            if total_count > 300:  # Увеличиваем паузу для больших батчей
                batch_pause *= 1.2
                
            logger.info(f"⏸️ Пауза между батчами: {batch_pause:.1f} сек")
            await asyncio.sleep(batch_pause)
            
            return len(results)
            
        except Exception as e:
            logger.error(f"Ошибка обработки смешанного батча: {e}")
            return 0
    
    async def _get_ready_mixed_tasks(self, current_time: float) -> List[Dict]:
        """Получает готовые задачи ЛЮБЫХ типов для смешанного батча"""
        try:
            batch_size = self.cached_settings['mixed_batch_size']
            
            # Получаем готовые задачи из Redis (ЛЮБЫЕ типы)
            ready_tasks_data = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max=current_time,
                withscores=True,
                start=0,
                num=batch_size  # Берем ровно столько, сколько нужно для батча
            )
            
            if not ready_tasks_data:
                return []
            
            mixed_tasks = []
            removed_broken_tasks = 0
            
            for task_json, score in ready_tasks_data:
                try:
                    task_data = json.loads(task_json)
                    
                    # Принимаем ЛЮБЫЕ типы задач
                    task_type = task_data.get('task_type')
                    if task_type in ['view', 'subscribe']:
                        task_data['redis_key'] = task_json
                        mixed_tasks.append(task_data)
                    else:
                        logger.warning(f"⚠️ Неизвестный тип задачи: {task_type}")
                        self.redis_client.zrem("task_queue", task_json)
                        removed_broken_tasks += 1
                        
                except Exception as e:
                    logger.error(f"Ошибка парсинга задачи: {e}")
                    self.redis_client.zrem("task_queue", task_json)
                    removed_broken_tasks += 1
            
            if removed_broken_tasks > 0:
                logger.warning(f"🗑️ Удалено {removed_broken_tasks} битых задач")
            
            # Удаляем взятые задачи из Redis
            for task in mixed_tasks:
                self.redis_client.zrem("task_queue", task['redis_key'])
                del task['redis_key']
            
            return mixed_tasks
            
        except Exception as e:
            logger.error(f"Ошибка получения смешанных задач: {e}")
            return []
    
    async def _execute_mixed_tasks_parallel(self, tasks: List[Dict]) -> List[bool]:
        """Выполняет смешанные задачи параллельно"""
        if not tasks:
            return []
        
        try:
            # Создаем параллельные задачи для ВСЕХ типов
            parallel_tasks = []
            
            for task in tasks:
                task_type = task.get('task_type')
                
                if task_type == 'view':
                    # Просмотры с новой логикой: Подключился → Пауза X1 → Просмотр X2 → Пауза X1 → Отключился
                    parallel_task = asyncio.create_task(
                        self._execute_single_view_task_new_logic(task)
                    )
                elif task_type == 'subscribe':
                    # Подписки как обычно
                    parallel_task = asyncio.create_task(
                        self._execute_single_subscribe_task(task)
                    )
                else:
                    logger.warning(f"⚠️ Неизвестный тип задачи: {task_type}")
                    continue
                
                parallel_tasks.append(parallel_task)
            
            # Выполняем ВСЕ задачи параллельно
            results = await asyncio.gather(*parallel_tasks, return_exceptions=True)
            
            # Анализируем результаты
            success_count = 0
            error_count = 0
            view_success = 0
            subscribe_success = 0
            
            for i, result in enumerate(results):
                task = tasks[i]
                phone = task.get('phone', 'unknown')
                channel = task.get('channel', 'unknown')
                task_type = task.get('task_type', 'unknown')
                
                if isinstance(result, Exception):
                    error_count += 1
                    logger.error(f"💥 {task_type.upper()} | {phone} | @{channel} | {result}")
                elif result:
                    success_count += 1
                    if task_type == 'view':
                        view_success += 1
                    elif task_type == 'subscribe':
                        subscribe_success += 1
                    logger.debug(f"✅ {task_type.upper()} | {phone} | @{channel}")
                else:
                    error_count += 1
                    logger.warning(f"❌ {task_type.upper()} | {phone} | @{channel}")
            
            # Обновляем упрощенную статистику
            self._update_simplified_time_stats(success_count)
            
            logger.info(f"📊 СМЕШАННЫЙ РЕЗУЛЬТАТ: 👀{view_success} 📺{subscribe_success} ❌{error_count}")
            
            return [r for r in results if not isinstance(r, Exception)]
            
        except Exception as e:
            logger.error(f"Ошибка выполнения смешанных задач: {e}")
            return []
    
    async def _execute_single_view_task_new_logic(self, task: Dict) -> bool:
        """
        Выполняет одну задачу просмотра с НОВОЙ логикой:
        1. Подключился
        2. Пауза X1 сек (view_connection_pause)
        3. Просмотр X2 сек (view_reading_time)  
        4. Пауза X1 сек (view_connection_pause)
        5. Отключился
        """
        session_data = task.get('account_session', '')
        phone = task.get('phone', 'unknown')
        channel = task.get('channel', 'unknown')
        post_id = task.get('post_id', 0)
        
        if not session_data:
            logger.warning(f"❌ {phone}: нет session_data")
            return False
        
        client = None
        
        try:
            # Получаем параметры из настроек
            connection_pause = self.cached_settings['view_connection_pause']  # X1
            reading_time = self.cached_settings['view_reading_time']          # X2
            
            # 1. ПОДКЛЮЧИЛСЯ к Telegram
            client = TelegramClient(
                StringSession(session_data),
                API_ID, API_HASH,
                lang_code=find_lang_code(task.get('lang', 'English')),
                connection_retries=1,
                timeout=20
            )
            
            await client.connect()
            
            # Проверяем авторизацию
            if not await client.is_user_authorized():
                logger.warning(f"❌ {phone}: не авторизован")
                await self._handle_task_failure(phone, 'view')
                return False
            
            # 2. ПАУЗА X1 секунд (пауза после подключения)
            await asyncio.sleep(connection_pause)            
            # Получаем entity канала и выполняем просмотр
            channel_entity = await client.get_entity(channel)
            await client(GetMessagesViewsRequest(
                peer=channel_entity,
                id=[post_id],
                increment=True
            ))
            
            await asyncio.sleep(reading_time)
            
            # 4. ПАУЗА X1 секунд (пауза перед отключением)
            await asyncio.sleep(connection_pause)
            await self._handle_task_success(phone)
            return True
            
        except FloodWaitError as e:
            logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s")
            await self._add_to_retry_queue(task, delay=e.seconds)
            return False
            
        except (RPCError, AuthKeyInvalidError) as e:
            logger.warning(f"❌ {phone}: критическая ошибка - {e}")
            await self._handle_task_failure(phone, 'view')
            return False
            
        except Exception as e:
            logger.error(f"💥 {phone}: неожиданная ошибка - {e}")
            await self._handle_task_failure(phone, 'view')
            return False
            
        finally:
            # 5. ВСЕГДА отключаемся
            if client:
                try:
                    await client.disconnect()
                    logger.debug(f"🔌 {phone}: отключился")
                except Exception as e:
                    logger.debug(f"Ошибка отключения клиента {phone}: {e}")
    
    async def _execute_single_subscribe_task(self, task: Dict) -> bool:
        """Выполняет одну задачу подписки (логика не изменилась)"""
        session_data = task.get('account_session', '')
        phone = task.get('phone', 'unknown')
        channel = task.get('channel', 'unknown')
        
        if not session_data:
            return False
        
        client = None
        
        try:
            client = TelegramClient(
                StringSession(session_data),
                API_ID, API_HASH,
                lang_code=find_lang_code(task.get('lang', 'English')),
                connection_retries=1,
                timeout=20
            )
            
            await client.connect()
            
            if not await client.is_user_authorized():
                await self._handle_task_failure(phone, 'subscribe')
                return False
            
            channel_entity = await client.get_entity(channel)
            await client(JoinChannelRequest(channel_entity))
            
            await asyncio.sleep(random.uniform(2, 5))
            
            logger.debug(f"✅ {phone}: подписан на @{channel}")
            await self._handle_task_success(phone)
            return True
            
        except Exception as e:
            logger.warning(f"❌ {phone}: ошибка подписки на @{channel} - {e}")
            await self._handle_task_failure(phone, 'subscribe')
            return False
            
        finally:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
    
    def _update_simplified_time_stats(self, successful_tasks: int):
        """Обновляет упрощенную временную статистику"""
        current_time = time.time()
        
        # Добавляем успешные задачи к текущей минуте
        self.performance_stats['tasks_current_minute'] += successful_tasks
        
        # Проверяем нужно ли обновить статистику по минутам
        if current_time - self.performance_stats['last_minute_update'] >= 60.0:
            # Сохраняем задачи за прошедшую минуту
            tasks_this_minute = self.performance_stats['tasks_current_minute']
            
            # Добавляем в обе очереди (60 минут и 24 часа)
            self.performance_stats['tasks_last_minute'].append(tasks_this_minute)
            self.performance_stats['tasks_last_24h'].append(tasks_this_minute)
            
            # Обновляем времена и сбрасываем счетчик
            self.performance_stats['last_minute_update'] = current_time
            self.performance_stats['tasks_current_minute'] = 0
            
            logger.debug(f"📊 За последнюю минуту выполнено {tasks_this_minute} задач")
    
    async def _save_simplified_stats_to_redis(self):
        """Сохраняет упрощенную статистику в Redis согласно требованиям"""
        try:
            current_time = time.time()
            
            # Статистика за последние 60 минут (для расчета среднего за секунду)
            tasks_last_hour = sum(self.performance_stats['tasks_last_minute'])
            
            # Статистика за последние 24 часа (для расчета времени выполнения)
            tasks_last_24h = sum(self.performance_stats['tasks_last_24h'])
            
            stats_data = {
                'timestamp': current_time,
                'tasks_last_hour': tasks_last_hour,      # Для расчета среднего за сек
                'tasks_last_24h': tasks_last_24h,        # Для расчета времени выполнения
                'uptime': current_time - self.performance_stats['start_time'],
                'restart_count': self.restart_count
            }
            
            # Сохраняем в Redis с TTL 10 минут
            self.redis_client.setex('worker_stats', 600, json.dumps(stats_data))
            
            logger.debug(f"📊 Упрощенная статистика сохранена: {tasks_last_hour}/час, {tasks_last_24h}/24ч")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения упрощенной статистики в Redis: {e}")
    
    # === СЛУЖЕБНЫЕ МЕТОДЫ ===
    
    async def _handle_task_success(self, phone: str):
        """Обрабатывает успешное выполнение задачи"""
        try:
            await reset_account_fails(phone)
            logger.debug(f"🔓 {phone}: сброшен счетчик ошибок")
        except Exception as e:
            logger.error(f"Ошибка обработки успеха для {phone}: {e}")
    
    async def _handle_task_failure(self, phone: str, task_type: str):
        """Обрабатывает неудачное выполнение задачи"""
        try:
            fail_count = await increment_account_fails(phone)
            
            if fail_count >= 3:
                await update_account_status(phone, 'ban')
                logger.warning(f"🚫 {phone}: переведен в BAN (неудач: {fail_count})")
            else:
                logger.debug(f"⚠️ {phone}: неудача {fail_count}/3 ({task_type})")
                
        except Exception as e:
            logger.error(f"Ошибка обработки неудачи для {phone}: {e}")
    
    async def _add_to_retry_queue(self, task: Dict, delay: int = 0):
        """Добавляет задачу в очередь повторов"""
        try:
            task['retry_count'] = task.get('retry_count', 0) + 1
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
            
            for _ in range(10):
                task_data = self.redis_client.rpop('retry_tasks')
                if not task_data:
                    break
                
                try:
                    task = json.loads(task_data)
                    
                    if task.get('retry_after', 0) <= current_time:
                        # Время пришло - выполняем
                        if task.get('task_type') == 'view':
                            success = await self._execute_single_view_task_new_logic(task)
                        else:
                            success = await self._execute_single_subscribe_task(task)
                            
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
                await self._update_cached_settings()
                logger.info("✅ Настройки обновлены")
            elif command['command'] == 'cleanup_tasks':
                logger.info("🗑️ Запрос очистки старых задач")
                await self._cleanup_old_tasks()
                
        except Exception as e:
            logger.error(f"Ошибка обработки команд: {e}")
    
    async def _check_banned_accounts_for_retry(self):
        """Проверяет забаненные аккаунты раз в 120 часов"""
        try:
            ban_accounts = await get_ban_accounts_for_retry()
            
            if not ban_accounts:
                logger.info("🔍 Нет забаненных аккаунтов для проверки")
                return
            
            logger.info(f"🔍 Проверяю {len(ban_accounts)} забаненных аккаунтов...")
            
            for account in ban_accounts[:5]:  # Проверяем не более 5 за раз
                phone = account['phone_number']
                
                try:
                    await mark_account_retry_attempt(phone)
                    
                    test_task = {
                        'account_session': account['session_data'],
                        'phone': phone,
                        'channel': 'telegram',
                        'lang': account['lang'],
                        'task_type': 'subscribe'
                    }
                    
                    success = await self._execute_single_subscribe_task(test_task)
                    
                    if success:
                        logger.info(f"🔓 {phone}: восстановлен из бана!")
                    else:
                        logger.info(f"🚫 {phone}: остается в бане")
                        
                    await asyncio.sleep(random.uniform(30, 60))
                    
                except Exception as e:
                    logger.error(f"Ошибка проверки забаненного аккаунта {phone}: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка проверки забаненных аккаунтов: {e}")
    
    async def _cleanup_old_tasks(self):
        """Очищает старые задачи из Redis"""
        try:
            current_time = time.time()
            cutoff_time = current_time - (48 * 3600)  # 48 часов назад
            
            # Очищаем старые задачи из основной очереди
            old_tasks = self.redis_client.zrangebyscore(
                "task_queue", 0, cutoff_time, start=0, num=1000
            )
            
            if old_tasks:
                for task_json in old_tasks:
                    self.redis_client.zrem("task_queue", task_json)
                    
                logger.info(f"🗑️ Очищено {len(old_tasks)} старых задач из основной очереди")
            
            # Очищаем старые retry задачи
            retry_tasks = self.redis_client.lrange('retry_tasks', 0, -1)
            cleaned_retry = 0
            
            for task_json in retry_tasks:
                try:
                    task = json.loads(task_json)
                    if task.get('created_at', 0) < cutoff_time:
                        self.redis_client.lrem('retry_tasks', 1, task_json)
                        cleaned_retry += 1
                except:
                    # Удаляем битые задачи
                    self.redis_client.lrem('retry_tasks', 1, task_json)
                    cleaned_retry += 1
            
            if cleaned_retry > 0:
                logger.info(f"🗑️ Очищено {cleaned_retry} старых retry задач")
            
        except Exception as e:
            logger.error(f"Ошибка очистки старых задач: {e}")
    
    async def _log_simple_stats(self):
        """Логирует упрощенную статистику"""
        try:
            total_in_redis = self.redis_client.zcard("task_queue") or 0
            current_time = time.time()
            ready_in_redis = self.redis_client.zcount("task_queue", 0, current_time) or 0
            retry_count = self.redis_client.llen("retry_tasks") or 0
            
            # УПРОЩЕННАЯ статистика согласно требованиям
            tasks_last_hour = sum(self.performance_stats['tasks_last_minute'])
            tasks_last_24h = sum(self.performance_stats['tasks_last_24h'])
            
            # Среднее количество задач за секунду (за последние 60 минут / 3600 секунд)
            avg_per_second = tasks_last_hour / 3600 if tasks_last_hour > 0 else 0
            
            # Ориентировочное время выполнения (все задачи за 24ч / задачи за 60мин)
            if tasks_last_hour > 0:
                estimated_hours = tasks_last_24h / tasks_last_hour
            else:
                estimated_hours = 0
            
            uptime = current_time - self.performance_stats['start_time']
            
            logger.info(f"""
📊 СТАТИСТИКА MIXED BATCH ВОРКЕРА (10 мин):
   
   🛡️ ВОССТАНОВЛЕНИЕ:
   🔄 Перезапусков: {self.restart_count}/{self.max_restarts}
   ⏰ Время работы: {uptime/3600:.1f} часов
   
   📦 СМЕШАННЫЕ БАТЧИ:
   📊 Размер батча: {self.cached_settings.get('mixed_batch_size', 500)} задач (любых типов)
   ⏸️ Пауза между батчами: {self.cached_settings.get('mixed_batch_pause', 30)}с
   
   📋 ОЧЕРЕДЬ:
   📋 Всего задач: {total_in_redis}
   ✅ Готовых сейчас: {ready_in_redis}
   🔄 Retry задач: {retry_count}
   
   ⚡ ПРОИЗВОДИТЕЛЬНОСТЬ (согласно требованиям):
   📈 За последние 60 минут: {tasks_last_hour} задач
   📈 За последние 24 часа: {tasks_last_24h} задач
   ⚡ Среднее задач/сек: {avg_per_second:.2f}
   ⏱️ Ориент. время выполнения: {estimated_hours:.1f}ч
   
   👀 НАСТРОЙКИ ПРОСМОТРОВ:
   📖 Время просмотра: {self.cached_settings.get('view_reading_time', 5)}с (X2)
   🔌 Пауза подключ/выкл: {self.cached_settings.get('view_connection_pause', 3)}с (X1)
   
   🕐 Время: {time.strftime('%H:%M:%S')}""")
                
        except Exception as e:
            logger.error(f"Ошибка логирования статистики: {e}")
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка mixed batch воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы mixed batch воркера...")
        
        try:
            await self._save_simplified_stats_to_redis()
            
            total_uptime = time.time() - self.performance_stats['start_time']
            tasks_total = sum(self.performance_stats['tasks_last_24h'])
            
            logger.info(f"""
📊 ФИНАЛЬНАЯ СТАТИСТИКА MIXED BATCH ВОРКЕРА:
   ⏰ Общее время работы: {total_uptime/3600:.1f} часов
   🔄 Всего перезапусков: {self.restart_count}
   ✅ Задач выполнено: {tasks_total}
   📦 Режим работы: Смешанные батчи (просмотры + подписки)
   🚀 Средняя производительность: {tasks_total/(total_uptime/3600):.1f} задач/час
            """)
            
            if self.redis_client:
                self.redis_client.close()
            
            await shutdown_db_pool()
            logger.info("✅ Mixed batch воркер корректно завершен")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении: {e}")

# Алиасы для обратной совместимости
SimpleTaskWorker = MixedBatchWorker
EnhancedTaskWorker = MixedBatchWorker
ResilientTaskWorker = MixedBatchWorker

# Запуск воркера
async def main():
    """Главная функция mixed batch воркера"""
    worker = MixedBatchWorker()
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("⏹️ Получен Ctrl+C, завершаем работу...")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        await worker.stop()

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        logger.info("✅ uvloop установлен для максимальной производительности")
    except ImportError:
        logger.info("⚠️ uvloop недоступен, используем стандартный event loop")
    
    logger.info("🚀 Запуск Mixed Batch Worker (просмотры + подписки в одном батче)...")
    asyncio.run(main())