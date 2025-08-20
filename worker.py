import asyncio
import logging
import time
import json
import random
import re
from typing import Dict, List, Optional
from collections import deque
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError, AuthKeyInvalidError
from telethon.tl.functions.messages import GetMessagesViewsRequest
from telethon.tl.functions.channels import JoinChannelRequest

from config import find_lang_code, API_ID, API_HASH, read_setting, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from database import (
    init_db_pool, shutdown_db_pool, update_account_status,
    increment_account_fails, reset_account_fails, 
    get_ban_accounts_for_retry, mark_account_retry_attempt
)
from session_manager import global_session_manager
from exceptions import SessionError, RateLimitError
from redis import Redis

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
        
        # Оптимизированный буфер
        self.task_buffer = deque()  # Очередь задач в памяти
        self.max_buffer_size = 1500  # Максимум задач в буфере
        self.min_buffer_size = 1400  # Минимум для догрузки
        
        # 🆕 НОВОЕ: Флаги для предотвращения блокировок
        self.buffer_updating = False  # Флаг актуализации буфера
        self.buffer_lock = asyncio.Lock()  # Лок для безопасности
        
        # Счетчики производительности
        self.processed_tasks = 0
        self.last_buffer_load = 0
        
        # 🆕 Кэш настроек для правильных задержек
        self.cached_settings = {}
        self.last_settings_update = 0
        
    async def start(self):
        logger.info("🚀 Запуск улучшенного Task Worker с асинхронной актуализацией...")
        
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
            
            # Загружаем настройки
            await self._update_cached_settings()
            
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
    
    async def _update_cached_settings(self):
        """Обновляет кэшированные настройки"""
        try:
            self.cached_settings = {
                'view_period': read_setting('followPeriod.txt', 3.0) * 3600,  # в секундах
                'view_delay': read_setting('delay.txt', 20.0) * 60,  # в секундах
                'sub_lag': read_setting('lag.txt', 14.0) * 60,  # в секундах 
                'sub_range': read_setting('range.txt', 5.0) * 60,  # в секундах
                'accounts_delay': read_setting('accounts_delay.txt', 2.0) * 60,  # в секундах
                'timeout_count': int(read_setting('timeout_count.txt', 3.0)),
                'timeout_duration': read_setting('timeout_duration.txt', 13.0) * 60  # в секундах
            }
            self.last_settings_update = time.time()
            
            logger.info(f"""
⚙️ НАСТРОЙКИ ВОРКЕРА ОБНОВЛЕНЫ:
   👀 Период просмотров: {self.cached_settings['view_period']/3600:.1f} часов
   ⏰ Задержка просмотров: {self.cached_settings['view_delay']/60:.1f} мин
   📺 Базовая задержка подписок: {self.cached_settings['sub_lag']/60:.1f} мин
   🎲 Разброс подписок: {self.cached_settings['sub_range']/60:.1f} мин
   ⏳ Между аккаунтами: {self.cached_settings['accounts_delay']/60:.1f} мин
   🔢 Подписок до паузы: {self.cached_settings['timeout_count']}
   ⏸️ Длительность паузы: {self.cached_settings['timeout_duration']/60:.1f} мин
            """)
            
        except Exception as e:
            logger.error(f"Ошибка обновления настроек: {e}")
    
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
        logger.info("🔄 Запуск цикла обработки задач с асинхронной актуализацией")
        
        last_stats_time = time.time()
        last_session_check = time.time()
        last_ban_check = time.time()
        last_buffer_info = time.time()
        buffer_check_counter = 0
        
        while self.running:
            try:
                # Проверяем команды от бота
                await self._process_worker_commands()
                
                # Обновляем настройки каждые 5 минут
                if time.time() - self.last_settings_update > 300:
                    await self._update_cached_settings()
                
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
                    # 🆕 НОВОЕ: Асинхронная актуализация буфера каждую минуту
                    buffer_check_counter += 1
                    if buffer_check_counter >= 30:  # 30 * 2 сек = 1 минута
                        # Запускаем актуализацию в фоне, не ждем завершения
                        if not self.buffer_updating:
                            asyncio.create_task(self._async_smart_buffer_management())
                        buffer_check_counter = 0
                    
                    # 🆕 АВТООЧИСТКА: Каждые 10 минут очищаем просроченные задачи
                    if random.random() < 0.001:  # ~0.1% шанс каждые 2 сек = раз в ~30 мин
                        asyncio.create_task(self._auto_cleanup_expired_tasks())
                    
                    await self._fill_task_buffer()
                    
                    # 🆕 НОВОЕ: Проверяем актуальность сессий перед выполнением
                    processed = await self._process_buffer_tasks_with_session_check()
                    self.processed_tasks += processed
                    
                    await self._process_retry_tasks()
                
                    if random.random() < 0.002:  # ~0.2% шанс
                        health_stats = await global_session_manager.health_check()
                        if health_stats.get('removed_dead', 0) > 0:
                            logger.info(f"🔧 Очищено {health_stats['removed_dead']} мертвых сессий")
                            # После очистки сессий, очищаем буфер от задач мертвых аккаунтов
                            asyncio.create_task(self._cleanup_buffer_dead_sessions())
                
                # Информация о буфере каждые 90 секунд
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
    
    async def _async_smart_buffer_management(self):
        """🆕 Асинхронная актуализация буфера без блокировки выполнения"""
        if self.buffer_updating:
            return
        
        self.buffer_updating = True
        try:
            async with self.buffer_lock:
                current_time = time.time()
                
                # Анализируем текущий буфер
                buffer_analysis = self._analyze_buffer_tasks()
                
                # Если в буфере много готовых задач, актуализацию не делаем
                if buffer_analysis['ready_now'] > 200:
                    logger.debug(f"🔒 Много готовых задач ({buffer_analysis['ready_now']}), актуализация отложена")
                    return
                
                # Получаем приоритетные задачи из Redis для анализа
                redis_priority_tasks = self.redis_client.zrangebyscore(
                    "task_queue",
                    min=0,
                    max='+inf',
                    withscores=True,
                    start=0,
                    num=2000
                )
                
                if not redis_priority_tasks:
                    logger.debug("📋 Нет задач в Redis для актуализации")
                    return
                
                # Выполняем умную актуализацию
                logger.debug(f"""
🧠 АСИНХРОННАЯ АКТУАЛИЗАЦИЯ БУФЕРА:
   📋 В буфере: {buffer_analysis['total']} задач
   ⏰ Готовых в буфере: {buffer_analysis['ready_now']}
   🕐 Готовых в 5 мин: {buffer_analysis['ready_5min']}
   📦 Задач в Redis: {len(redis_priority_tasks)}
                """)
                
                await self._reorder_buffer_tasks_safe(redis_priority_tasks, current_time)
        
        except Exception as e:
            logger.error(f"Ошибка асинхронной актуализации буфера: {e}")
        finally:
            self.buffer_updating = False
    
    async def _reorder_buffer_tasks_safe(self, redis_priority_tasks: List, current_time: float):
        """Безопасная умная актуализация буфера с защитой готовых задач"""
        try:
            # 1. Разделяем задачи в буфере на готовые и будущие
            ready_tasks = []  # Готовые к выполнению - оставляем
            future_tasks = []  # Будущие - возвращаем в Redis для актуализации
            
            # Создаем временную копию буфера для безопасной работы
            buffer_copy = list(self.task_buffer)
            
            for task in buffer_copy:
                execute_at = task.get('execute_at', current_time)
                if execute_at <= current_time + 30:  # Готовые сейчас + 30 сек запас
                    ready_tasks.append(task)
                else:
                    future_tasks.append(task)
            
            # 2. Возвращаем только будущие задачи в Redis
            returned_count = 0
            if future_tasks:
                redis_return_data = {}
                for task in future_tasks:
                    task_json = json.dumps(task)
                    execute_at = task.get('execute_at', current_time)
                    redis_return_data[task_json] = execute_at
                
                if redis_return_data:
                    self.redis_client.zadd("task_queue", redis_return_data)
                    returned_count = len(future_tasks)
            
            # 3. Определяем сколько новых задач нужно загрузить
            spots_available = self.max_buffer_size - len(ready_tasks)
            
            if spots_available <= 200:  # Если мало места, оставляем как есть
                self.task_buffer = deque(ready_tasks)
                logger.debug(f"🔒 Буфер заполнен готовыми задачами, актуализация отложена")
                return
            
            # 4. Загружаем самые актуальные задачи из Redis
            fresh_tasks = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max='+inf',
                withscores=True,
                start=0,
                num=spots_available
            )
            
            loaded_count = 0
            new_tasks = []
            
            for task_json, score in fresh_tasks:
                try:
                    task_data = json.loads(task_json)
                    task_data['score'] = score
                    
                    # Проверяем что сессия аккаунта еще активна
                    session_data = task_data.get('account_session')
                    if session_data and global_session_manager.get_client(session_data):
                        new_tasks.append(task_data)
                        loaded_count += 1
                        # Удаляем из Redis
                        self.redis_client.zrem("task_queue", task_json)
                    else:
                        # Удаляем задачу с неактивной сессией
                        self.redis_client.zrem("task_queue", task_json)
                        logger.debug(f"🗑️ Удалена задача с неактивной сессией: {task_data.get('phone', 'unknown')}")
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки актуальной задачи: {e}")
                    # Удаляем битую задачу
                    self.redis_client.zrem("task_queue", task_json)
            
            # 5. Безопасно обновляем буфер
            all_tasks = ready_tasks + new_tasks
            if all_tasks:
                sorted_tasks = sorted(all_tasks, key=lambda x: x.get('execute_at', current_time))
                self.task_buffer = deque(sorted_tasks)
            else:
                self.task_buffer = deque()
            
            # 6. Статистика умной актуализации
            ready_now = sum(1 for t in self.task_buffer if t.get('execute_at', 0) <= current_time)
            ready_5min = sum(1 for t in self.task_buffer 
                            if current_time < t.get('execute_at', 0) <= current_time + 300)
            
            logger.info(f"""
✅ АСИНХРОННАЯ АКТУАЛИЗАЦИЯ ЗАВЕРШЕНА:
   🔒 Сохранено готовых: {len(ready_tasks)} задач
   🔄 Возвращено в Redis: {returned_count} задач
   📥 Загружено свежих: {loaded_count} задач  
   📋 Итого в буфере: {len(self.task_buffer)}/1500
   ⏰ Готовых сейчас: {ready_now}
   🕐 Готовых в 5 мин: {ready_5min}
            """)
        
        except Exception as e:
            logger.error(f"Ошибка безопасной актуализации буфера: {e}")
    
    async def _cleanup_buffer_dead_sessions(self):
        """🆕 Очищает буфер от задач с мертвыми сессиями"""
        try:
            async with self.buffer_lock:
                alive_tasks = []
                removed_count = 0
                
                for task in self.task_buffer:
                    session_data = task.get('account_session')
                    if session_data and global_session_manager.get_client(session_data):
                        alive_tasks.append(task)
                    else:
                        removed_count += 1
                
                if removed_count > 0:
                    self.task_buffer = deque(alive_tasks)
                    logger.info(f"🧹 Очищено {removed_count} задач с мертвыми сессиями из буфера")
                    
        except Exception as e:
            logger.error(f"Ошибка очистки мертвых сессий из буфера: {e}")
    
    def _analyze_buffer_tasks(self) -> Dict:
        """Простой анализ задач в буфере"""
        current_time = time.time()
        
        analysis = {
            'total': len(self.task_buffer),
            'ready_now': 0,
            'ready_5min': 0,
            'future': 0
        }
        
        for task in self.task_buffer:
            execute_at = task.get('execute_at', current_time)
            
            if execute_at <= current_time:
                analysis['ready_now'] += 1
            elif execute_at <= current_time + 300:  # 5 минут
                analysis['ready_5min'] += 1
            else:
                analysis['future'] += 1
        
        return analysis
    
    async def _fill_task_buffer(self):
        """Загружает готовые задачи из Redis в буфер памяти"""
        try:
            # Проверяем нужно ли загружать (убираем проверку на актуализацию)
            if len(self.task_buffer) >= self.min_buffer_size:
                return
            
            current_time = time.time()
            
            # Сколько задач нужно загрузить
            needed = self.max_buffer_size - len(self.task_buffer)
            
            # 🆕 ИСПРАВЛЕНИЕ: Получаем готовые задачи с запасом времени
            ready_tasks_data = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max=current_time + 60,  # +1 минута запас для готовых задач
                withscores=True,
                start=0,
                num=needed
            )
            
            if not ready_tasks_data:
                # Если нет готовых, загружаем ближайшие будущие
                ready_tasks_data = self.redis_client.zrangebyscore(
                    "task_queue",
                    min=current_time,
                    max=current_time + 300,  # +5 минут
                    withscores=True,
                    start=0,
                    num=min(needed, 100)  # Не более 100 будущих
                )
            
            if not ready_tasks_data:
                return
            
            loaded_count = 0
            
            # Используем блокировку только для обновления буфера
            new_tasks = []
            
            for task_json, score in ready_tasks_data:
                try:
                    task_data = json.loads(task_json)
                    task_data['score'] = score
                    
                    # Проверяем что сессия активна
                    session_data = task_data.get('account_session')
                    if session_data and global_session_manager.get_client(session_data):
                        new_tasks.append(task_data)
                        loaded_count += 1
                    
                    # Удаляем из Redis в любом случае
                    self.redis_client.zrem("task_queue", task_json)
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки задачи в буфер: {e}")
                    # Удаляем битую задачу
                    self.redis_client.zrem("task_queue", task_json)
            
            # Быстро добавляем в буфер
            if new_tasks:
                try:
                    # Кратковременная блокировка только для добавления
                    async with asyncio.timeout(1.0):  # Максимум 1 секунда блокировки
                        async with self.buffer_lock:
                            self.task_buffer.extend(new_tasks)
                except asyncio.TimeoutError:
                    logger.warning("⚠️ Timeout при добавлении задач в буфер, добавляю без блокировки")
                    # Добавляем без блокировки в критической ситуации
                    self.task_buffer.extend(new_tasks)
            
            if loaded_count > 0:
                self.last_buffer_load = time.time()
                logger.info(f"📥 ЗАГРУЖЕНО {loaded_count} задач в буфер (готовых: {sum(1 for t in new_tasks if t.get('execute_at', 0) <= current_time + 60)})")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки задач в буфер: {e}")
    
    async def _process_buffer_tasks_with_session_check(self) -> int:
        """🆕 Обрабатывает готовые задачи из буфера БЕЗ блокировки на просроченных"""
        if not self.task_buffer:
            return 0
        
        current_time = time.time()
        processed_count = 0
        
        # 🆕 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Ищем готовые задачи по всему буферу
        expired_threshold = current_time - 3600  # 1 час назад
        ready_tasks = []
        future_tasks = []
        expired_tasks = []
        
        # 🚀 НОВАЯ ЛОГИКА: Сначала анализируем весь буфер
        buffer_copy = list(self.task_buffer)
        self.task_buffer.clear()
        
        for task in buffer_copy:
            execute_at = task.get('execute_at', 0)
            
            if execute_at < expired_threshold:
                # Сильно просроченные - пропускаем совсем
                expired_tasks.append(task)
            elif execute_at <= current_time + 60:  # Готовые + 1 минута запас
                # Готовые к выполнению
                ready_tasks.append(task)
            else:
                # Будущие - возвращаем в буфер
                future_tasks.append(task)
        
        # Возвращаем будущие задачи в буфер (сортированные)
        future_tasks.sort(key=lambda x: x.get('execute_at', 0))
        self.task_buffer.extend(future_tasks)
        
        # 🚀 ПАРАЛЛЕЛЬНОЕ ВЫПОЛНЕНИЕ ГОТОВЫХ ЗАДАЧ
        executed_count = 0
        skipped_no_session = 0
        
        if ready_tasks:
            logger.info(f"🚀 ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА {len(ready_tasks)} готовых задач...")
            
            # Разделяем на батчи для параллельной обработки
            batch_size = 20  # 20 задач параллельно
            total_batches = (len(ready_tasks) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(ready_tasks))
                batch_tasks = ready_tasks[start_idx:end_idx]
                
                logger.info(f"⚡ Батч {batch_num + 1}/{total_batches}: {len(batch_tasks)} задач")
                
                # Создаем параллельные задачи
                parallel_tasks = []
                for task in batch_tasks:
                    # Проверяем сессию перед добавлением в параллельный пул
                    session_data = task.get('account_session')
                    if session_data and global_session_manager.get_client(session_data):
                        parallel_task = asyncio.create_task(
                            self._execute_single_task_parallel(task)
                        )
                        parallel_tasks.append((parallel_task, task))
                    else:
                        skipped_no_session += 1
                
                # Выполняем батч параллельно
                if parallel_tasks:
                    batch_results = await asyncio.gather(
                        *[task for task, _ in parallel_tasks], 
                        return_exceptions=True
                    )
                    
                    # Анализируем результаты батча
                    for (task_coro, task_data), result in zip(parallel_tasks, batch_results):
                        task_type = task_data.get('task_type', '')
                        phone = task_data.get('phone', 'unknown')
                        channel = task_data.get('channel', 'unknown')
                        
                        if isinstance(result, Exception):
                            logger.error(f"💥 ОШИБКА: {task_type} | {phone} | {result}")
                        elif result:
                            executed_count += 1
                            logger.debug(f"✅ ВЫПОЛНЕНО: {task_type} | {phone} | @{channel}")
                        else:
                            logger.warning(f"❌ НЕ ВЫПОЛНЕНО: {task_type} | {phone} | @{channel}")
                
                # Пауза между батчами (для подписок больше)
                has_subscriptions = any(t.get('task_type') == 'subscribe' for t in batch_tasks)
                batch_delay = random.uniform(3.0, 6.0) if has_subscriptions else random.uniform(0.5, 1.5)
                
                if batch_num < total_batches - 1:  # Не ждем после последнего батча
                    logger.debug(f"⏳ Пауза между батчами: {batch_delay:.1f}с")
                    await asyncio.sleep(batch_delay)
        
        # Если остались необработанные готовые задачи (больше чем помещается в батчи)
        # В новой логике мы обрабатываем ВСЕ готовые задачи, поэтому remaining_ready всегда пустой
        remaining_ready = []
        if remaining_ready:
            # Добавляем в начало буфера
            remaining_ready.extend(list(self.task_buffer))
            self.task_buffer = deque(remaining_ready)
        
        # Логируем подробную статистику
        if executed_count > 0 or len(expired_tasks) > 0 or skipped_no_session > 0:
            success_rate = (executed_count / len(ready_tasks)) * 100 if ready_tasks else 0
            
            logger.info(f"""
📊 ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ЗАВЕРШЕНА:
   ✅ Выполнено: {executed_count}/{len(ready_tasks)} ({success_rate:.1f}%)
   ⚠️ Пропущено (нет сессии): {skipped_no_session}
   🗑️ Просроченных (>1ч): {len(expired_tasks)} УДАЛЕНО
   ⏳ Будущих: {len(future_tasks)}
   📦 Всего в буфере: {len(self.task_buffer)}
   ⚡ Режим: Параллельный (батчи по 20)
            """)
        
        # Логируем просроченные для отладки и УДАЛЯЕМ их из Redis
        if expired_tasks:
            logger.info(f"🗑️ УДАЛЯЮ {len(expired_tasks)} просроченных задач из Redis...")
            
            for task in expired_tasks:
                task_type = task.get('task_type', '')
                phone = task.get('phone', 'unknown')
                channel = task.get('channel', 'unknown')
                delay_hours = (current_time - task.get('execute_at', 0)) / 3600
                
                # Логируем что удаляем
                logger.warning(f"🗑️ УДАЛЕНА ПРОСРОЧЕННАЯ: {task_type} | {phone} | @{channel} | {delay_hours:.1f}ч")
                
                # 🆕 НОВОЕ: Удаляем из Redis тоже
                try:
                    task_json = json.dumps(task)
                    result = self.redis_client.zrem("task_queue", task_json)
                    if result:
                        logger.debug(f"✅ Удалена из Redis: {phone}")
                except Exception as e:
                    logger.error(f"❌ Ошибка удаления из Redis {phone}: {e}")
        
        return executed_count
    
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
            reading_time = random.uniform(3, 7)
            await asyncio.sleep(reading_time)
            
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
            logger.error(f"💥 {phone}: неожиданная ошибка просмотра - {e}")
            await self._handle_task_failure(phone, 'view')
            return False

    async def _execute_subscription_task(self, task: Dict) -> bool:
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
            
            
            created_at = task.get('created_at', 0)
            execute_at = task.get('execute_at', 0)
            current_time = time.time()
            
            actual_delay = (current_time - created_at) / 60  # в минутах
            planned_delay = (execute_at - created_at) / 60   # в минутах
            execution_drift = (current_time - execute_at) / 60  # отклонение от плана
            
            # Определяем статус выполнения по времени
            if abs(execution_drift) <= 1.0:
                timing_status = "⏰ ТОЧНО"
            elif execution_drift < 0:
                timing_status = f"🚀 РАНО ({abs(execution_drift):.1f}мин)"
            else:
                timing_status = f"⏳ ПОЗДНО (+{execution_drift:.1f}мин)"
            
            # Определяем цвет для логирования в зависимости от точности
            if abs(execution_drift) <= 2.0:
                log_level = logger.info  # Зеленый - хорошая точность
            elif abs(execution_drift) <= 5.0:
                log_level = logger.warning  # Желтый - приемлемое отклонение
            else:
                log_level = logger.error  # Красный - большое отклонение
            
            log_level(f"""
📺 ВЫПОЛНЕНИЕ ПОДПИСКИ:
   📱 Аккаунт: {phone}
   📺 Канал: @{channel}
   📅 Создана: {time.strftime('%H:%M:%S', time.localtime(created_at))}
   ⏰ Планировалась: {time.strftime('%H:%M:%S', time.localtime(execute_at))} (через {planned_delay:.1f}мин)
   🕐 Выполняется: {time.strftime('%H:%M:%S', time.localtime(current_time))} (через {actual_delay:.1f}мин)
   📊 Статус: {timing_status}
   ⚙️ Настройки: lag={self.cached_settings.get('sub_lag', 840)/60:.1f}мин, range=±{self.cached_settings.get('sub_range', 300)/60:.1f}мин
            """)
            
            # Выполняем подписку
            await client(JoinChannelRequest(channel_entity))
            
            # Дополнительная статистика для мониторинга
            subscription_hour = time.strftime('%H', time.localtime(current_time))
            logger.debug(f"📊 Подписка в {subscription_hour}:00 | Канал: @{channel} | Отклонение: {execution_drift:.1f}мин")
            
            # Успех
            await self._handle_task_success(phone)
            
            # Финальное сообщение с кратким статусом
            if abs(execution_drift) <= 1.0:
                logger.info(f"✅ {phone}: подписан на @{channel} | {timing_status}")
            elif abs(execution_drift) <= 5.0:
                logger.warning(f"⚠️ {phone}: подписан на @{channel} | {timing_status}")
            else:
                logger.error(f"🔥 {phone}: подписан на @{channel} | {timing_status} - ПРОВЕРИТЬ НАСТРОЙКИ!")
            
            return True
            
        except FloodWaitError as e:
            logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s при подписке на @{channel}")
            await self._add_to_retry_queue(task, 'subscription', delay=e.seconds)
            return False
            
        except (RPCError, AuthKeyInvalidError) as e:
            logger.warning(f"❌ {phone}: критическая ошибка подписки на @{channel} - {e}")
            await self._handle_task_failure(phone, 'subscription')
            return False
            
        except Exception as e:
            logger.error(f"💥 {phone}: неожиданная ошибка подписки на @{channel} - {e}")
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
        """Обрабатывает неудачное выполнение задачи (обновленная версия)"""
        try:
            fail_count = await increment_account_fails(phone)
            
            if fail_count >= 3:
                await update_account_status(phone, 'ban')
                logger.warning(f"🚫 {phone}: переведен в BAN (неудач: {fail_count})")
                
                # Удаляем сессию из пула
                await global_session_manager.remove_session_by_phone(phone)
                
                # 🆕 НОВОЕ: Удаляем все задачи этого аккаунта асинхронно
                asyncio.create_task(self._remove_tasks_for_banned_account_async(phone))
            else:
                logger.debug(f"⚠️ {phone}: неудача {fail_count}/3 ({task_type})")
                
        except Exception as e:
            logger.error(f"Ошибка обработки неудачи для {phone}: {e}")
    
    async def _remove_tasks_for_banned_account_async(self, phone: str):
        """🆕 Асинхронно удаляет все задачи забаненного аккаунта"""
        try:
            removed_from_redis = 0
            removed_from_buffer = 0
            
            # 1. Удаляем из буфера памяти (быстро)
            async with self.buffer_lock:
                buffer_tasks_to_keep = []
                for task in self.task_buffer:
                    if task.get('phone') != phone:
                        buffer_tasks_to_keep.append(task)
                    else:
                        removed_from_buffer += 1
                
                self.task_buffer = deque(buffer_tasks_to_keep)
            
            # 2. Удаляем из Redis очереди (может быть медленно, делаем асинхронно)
            # Используем SCAN для избежания блокировки Redis
            cursor = 0
            while True:
                cursor, keys = self.redis_client.zscan("task_queue", cursor, count=100)
                
                for task_json, score in keys.items():
                    try:
                        task_data = json.loads(task_json)
                        if task_data.get('phone') == phone:
                            self.redis_client.zrem("task_queue", task_json)
                            removed_from_redis += 1
                    except Exception as e:
                        logger.error(f"Ошибка обработки задачи Redis: {e}")
                
                if cursor == 0:
                    break
                
                # Небольшая пауза чтобы не блокировать Redis
                await asyncio.sleep(0.01)
            
            # 3. Удаляем из retry очереди
            retry_tasks = []
            removed_from_retry = 0
            
            # Получаем все retry задачи
            all_retry_tasks = self.redis_client.lrange('retry_tasks', 0, -1)
            
            for retry_task_json in all_retry_tasks:
                try:
                    retry_task = json.loads(retry_task_json)
                    if retry_task.get('phone') != phone:
                        retry_tasks.append(retry_task_json)
                    else:
                        removed_from_retry += 1
                except:
                    retry_tasks.append(retry_task_json)  # Сохраняем битые задачи
            
            # Заменяем всю retry очередь
            if all_retry_tasks:
                self.redis_client.delete('retry_tasks')
                if retry_tasks:
                    self.redis_client.lpush('retry_tasks', *retry_tasks)
            
            total_removed = removed_from_redis + removed_from_buffer + removed_from_retry
            
            if total_removed > 0:
                logger.info(f"""
🗑️ АСИНХРОННО очищены задачи забаненного аккаунта {phone}:
   📦 Из Redis: {removed_from_redis}
   💾 Из буфера: {removed_from_buffer}  
   🔄 Из retry: {removed_from_retry}
   📊 Всего удалено: {total_removed}
                """)
            
        except Exception as e:
            logger.error(f"Ошибка асинхронной очистки задач для {phone}: {e}")
    
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
                        # Проверяем что сессия еще активна
                        session_data = task.get('account_session')
                        if session_data and global_session_manager.get_client(session_data):
                            # Время пришло и сессия активна - выполняем
                            success = await self._execute_task(task)
                            if success:
                                logger.debug(f"✅ Retry задача выполнена успешно")
                        else:
                            logger.debug(f"🗑️ Retry задача отброшена - неактивная сессия")
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
            
            elif command['command'] == 'buffer_diagnostic':
                logger.info("🔍 ДИАГНОСТИКА БУФЕРА:")
                await self._diagnostic_buffer_content()
            
            elif command['command'] == 'force_process_buffer':
                logger.info("⚡ ПРИНУДИТЕЛЬНОЕ ВЫПОЛНЕНИЕ БУФЕРА:")
                processed = await self._force_process_buffer()
                logger.info(f"⚡ Принудительно выполнено: {processed} задач")
            
            elif command['command'] == 'cleanup_expired_buffer':
                max_delay_hours = command.get('max_delay_hours', 1.0)
                logger.info(f"🗑️ ОЧИСТКА ПРОСРОЧЕННЫХ ЗАДАЧ БУФЕРА (>{max_delay_hours}ч):")
                cleaned = await self._cleanup_expired_buffer_tasks(max_delay_hours)
                logger.info(f"🗑️ Очищено из буфера: {cleaned} просроченных задач")
                
        except Exception as e:
            logger.error(f"Ошибка обработки команд: {e}")
    
    async def _diagnostic_buffer_content(self):
        """🆕 Диагностика содержимого буфера"""
        try:
            current_time = time.time()
            buffer_size = len(self.task_buffer)
            
            logger.info(f"📋 СОДЕРЖИМОЕ БУФЕРА ({buffer_size} задач):")
            logger.info(f"🔄 buffer_updating: {self.buffer_updating}")
            logger.info(f"🔒 buffer_lock: {'заблокирован' if self.buffer_lock.locked() else 'свободен'}")
            
            if buffer_size == 0:
                logger.info("📋 БУФЕР ПУСТОЙ!")
                return
            
            # Анализируем первые 10 задач
            ready_count = 0
            future_count = 0
            invalid_count = 0
            no_session_count = 0
            
            logger.info("🔍 АНАЛИЗ ПЕРВЫХ 10 ЗАДАЧ:")
            
            for i, task in enumerate(list(self.task_buffer)[:10]):
                execute_at = task.get('execute_at', 0)
                phone = task.get('phone', 'unknown')
                task_type = task.get('task_type', 'unknown')
                session_data = task.get('account_session', '')
                
                # Проверяем время
                time_diff = execute_at - current_time
                
                # Проверяем сессию
                has_session = bool(session_data and global_session_manager.get_client(session_data))
                
                if execute_at <= current_time:
                    ready_count += 1
                    status = "✅ ГОТОВО"
                elif execute_at <= current_time + 30:
                    ready_count += 1
                    status = f"⏰ ГОТОВО ({time_diff:.1f}с)"
                else:
                    future_count += 1
                    status = f"⏳ БУДУЩЕЕ ({time_diff/60:.1f}мин)"
                
                if not has_session:
                    no_session_count += 1
                    session_status = "❌ НЕТ СЕССИИ"
                else:
                    session_status = "✅ СЕССИЯ ОК"
                
                if execute_at <= 0 or not phone or not task_type:
                    invalid_count += 1
                    task_status = "💥 БИТАЯ"
                else:
                    task_status = "✅ ОК"
                
                logger.info(f"   {i+1}. {task_type} | {phone} | {status} | {session_status} | {task_status}")
            
            # Общая статистика буфера
            total_ready = sum(1 for task in self.task_buffer if task.get('execute_at', 0) <= current_time + 30)
            total_future = buffer_size - total_ready
            total_no_session = sum(1 for task in self.task_buffer 
                                 if not (task.get('account_session') and 
                                        global_session_manager.get_client(task.get('account_session'))))
            
            logger.info(f"""
📊 ОБЩАЯ СТАТИСТИКА БУФЕРА:
   📋 Всего задач: {buffer_size}
   ✅ Готовых к выполнению: {total_ready}
   ⏳ Будущих: {total_future}
   ❌ Без активной сессии: {total_no_session}
   💥 Битых в первых 10: {invalid_count}
   🔒 Блокировка актуализации: {self.buffer_updating}
            """)
            
        except Exception as e:
            logger.error(f"Ошибка диагностики буфера: {e}")
    
    async def _force_process_buffer(self) -> int:
        """🆕 Принудительная обработка буфера для диагностики"""
        try:
            current_time = time.time()
            processed = 0
            
            logger.info("⚡ НАЧИНАЮ ПРИНУДИТЕЛЬНУЮ ОБРАБОТКУ БУФЕРА")
            
            if not self.task_buffer:
                logger.info("📋 Буфер пуст, нечего обрабатывать")
                return 0
            
            # Пробуем обработать до 10 задач принудительно
            for attempt in range(10):
                if not self.task_buffer:
                    break
                
                # Берем задачу БЕЗ блокировки
                try:
                    task = self.task_buffer.popleft()
                except:
                    logger.info("❌ Не удалось взять задачу из буфера")
                    break
                
                execute_at = task.get('execute_at', 0)
                phone = task.get('phone', 'unknown')
                task_type = task.get('task_type', 'unknown')
                channel = task.get('channel', 'unknown')
                session_data = task.get('account_session', '')
                
                logger.info(f"⚡ Попытка {attempt+1}: {task_type} | {phone} | @{channel}")
                
                # Проверяем время
                if execute_at > current_time + 60:
                    logger.info(f"   ⏰ Задача слишком в будущем ({(execute_at-current_time)/60:.1f} мин)")
                    self.task_buffer.appendleft(task)  # Возвращаем
                    break
                
                # Проверяем сессию
                if not session_data:
                    logger.info(f"   ❌ Нет session_data")
                    continue
                
                client = global_session_manager.get_client(session_data)
                if not client:
                    logger.info(f"   ❌ get_client() вернул None")
                    continue
                
                # Пробуем выполнить
                try:
                    logger.info(f"   🚀 Выполняю задачу...")
                    success = await self._execute_task(task)
                    
                    if success:
                        processed += 1
                        logger.info(f"   ✅ УСПЕХ! Задача выполнена")
                    else:
                        logger.info(f"   ❌ _execute_task() вернул False")
                    
                    # Пауза между попытками
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.info(f"   💥 Исключение при выполнении: {e}")
                    continue
            
            logger.info(f"⚡ ПРИНУДИТЕЛЬНАЯ ОБРАБОТКА ЗАВЕРШЕНА: {processed} задач выполнено")
            return processed
            
        except Exception as e:
            logger.error(f"Ошибка принудительной обработки: {e}")
            return 0
    
    async def _cleanup_expired_buffer_tasks(self, max_delay_hours=1.0) -> int:
        """🆕 Очищает просроченные задачи из буфера"""
        try:
            current_time = time.time()
            cutoff_time = current_time - (max_delay_hours * 3600)
            
            logger.info(f"🗑️ Очистка задач просроченных >{max_delay_hours} часов")
            logger.info(f"📊 Задач в буфере до очистки: {len(self.task_buffer)}")
            
            if not self.task_buffer:
                logger.info("📋 Буфер пуст, нечего очищать")
                return 0
            
            # Анализируем буфер
            valid_tasks = []
            expired_tasks = []
            broken_tasks = []
            
            # Проходим по всем задачам в буфере
            while self.task_buffer:
                try:
                    task = self.task_buffer.popleft()
                    
                    execute_at = task.get('execute_at', 0)
                    task_type = task.get('task_type', 'unknown')
                    phone = task.get('phone', 'unknown')
                    channel = task.get('channel', 'unknown')
                    
                    # Проверяем валидность задачи
                    if not execute_at or not phone or not task_type:
                        broken_tasks.append(task)
                        continue
                    
                    # Проверяем просрочку
                    if execute_at < cutoff_time:
                        expired_tasks.append(task)
                        delay_hours = (current_time - execute_at) / 3600
                        logger.debug(f"🗑️ Просрочена: {task_type} | {phone} | @{channel} | {delay_hours:.1f}ч")
                    else:
                        valid_tasks.append(task)
                
                except Exception as e:
                    logger.error(f"Ошибка анализа задачи в буфере: {e}")
                    broken_tasks.append(task)
            
            # Возвращаем валидные задачи в буфер
            self.task_buffer = deque(valid_tasks)
            
            cleaned_count = len(expired_tasks) + len(broken_tasks)
            
            logger.info(f"""
🗑️ РЕЗУЛЬТАТ ОЧИСТКИ БУФЕРА:
   📋 Было задач: {len(valid_tasks) + cleaned_count}
   ✅ Оставлено валидных: {len(valid_tasks)}
   🗑️ Удалено просроченных: {len(expired_tasks)}
   💥 Удалено битых: {len(broken_tasks)}
   📊 Итого очищено: {cleaned_count}
            """)
            
            # Статистика по типам очищенных задач
            if expired_tasks:
                type_stats = {}
                for task in expired_tasks:
                    task_type = task.get('task_type', 'unknown')
                    type_stats[task_type] = type_stats.get(task_type, 0) + 1
                
                logger.info("📊 Очищенные задачи по типам:")
                for task_type, count in type_stats.items():
                    logger.info(f"   {task_type}: {count}")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Ошибка очистки просроченных задач буфера: {e}")
            return 0
    
    async def _auto_cleanup_expired_tasks(self):
        """🆕 Автоматическая агрессивная очистка просроченных задач"""
        try:
            # 🆕 АГРЕССИВНАЯ ОЧИСТКА: Очищаем задачи просроченные больше чем на 30 минут
            cleaned_buffer = await self._cleanup_expired_buffer_tasks(max_delay_hours=0.5)
            
            # Очищаем Redis более агрессивно
            current_time = time.time()
            cutoff_time = current_time - 1800  # 30 минут назад (более агрессивно)
            
            # Получаем просроченные задачи из Redis
            expired_redis_tasks = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max=cutoff_time,
                start=0,
                num=2000  # Увеличиваем лимит
            )
            
            cleaned_redis = 0
            if expired_redis_tasks:
                logger.info(f"🗑️ Найдено {len(expired_redis_tasks)} просроченных задач в Redis")
                
                # Удаляем просроченные из Redis батчами
                for i in range(0, len(expired_redis_tasks), 100):
                    batch = expired_redis_tasks[i:i+100]
                    
                    for task_json in batch:
                        try:
                            # Анализируем задачу перед удалением
                            task_data = json.loads(task_json)
                            task_type = task_data.get('task_type', 'unknown')
                            phone = task_data.get('phone', 'unknown')
                            execute_at = task_data.get('execute_at', 0)
                            delay_hours = (current_time - execute_at) / 3600
                            
                            if delay_hours > 0.5:  # Просрочена больше 30 минут
                                self.redis_client.zrem("task_queue", task_json)
                                cleaned_redis += 1
                                logger.debug(f"🗑️ Redis: удалена {task_type} | {phone} | {delay_hours:.1f}ч")
                                
                        except Exception as e:
                            # Удаляем битые задачи тоже
                            self.redis_client.zrem("task_queue", task_json)
                            cleaned_redis += 1
                            logger.debug(f"🗑️ Redis: удалена битая задача")
                    
                    # Небольшая пауза между батчами
                    await asyncio.sleep(0.1)
            
            total_cleaned = cleaned_buffer + cleaned_redis
            
            if total_cleaned > 0:
                logger.info(f"""
🗑️ АГРЕССИВНАЯ АВТООЧИСТКА (>30 мин):
   💾 Из буфера: {cleaned_buffer}
   📦 Из Redis: {cleaned_redis}
   📊 Всего очищено: {total_cleaned}
   ⏰ Порог: 30 минут просрочки
                """)
                
        except Exception as e:
            logger.error(f"Ошибка агрессивной автоочистки: {e}")
    
    async def _execute_single_task_parallel(self, task: Dict) -> bool:
        """🚀 Выполняет одну задачу для параллельной обработки"""
        task_type = task.get('task_type', '')
        phone = task.get('phone', 'unknown')
        
        try:
            # Добавляем небольшую случайную задержку в начале для разброса
            initial_delay = random.uniform(0.05, 0.3)
            await asyncio.sleep(initial_delay)
            
            # Выполняем задачу
            success = await self._execute_task(task)
            
            if success:
                # Логируем только каждую 10-ю успешную задачу чтобы не засорять логи
                if random.random() < 0.1:  # 10% шанс
                    channel = task.get('channel', 'unknown')
                    logger.info(f"✅ ВЫПОЛНЕНО: {task_type} | {phone} | @{channel}")
                
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"💥 Параллельная ошибка {task_type} | {phone}: {e}")
            return False
    
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
                
                # Статистика задержек для подписок
                subscription_delays = []
                for task in self.task_buffer:
                    if task.get('task_type') == 'subscribe':
                        created_at = task.get('created_at', current_time)
                        execute_at = task.get('execute_at', current_time)
                        delay_minutes = (execute_at - created_at) / 60
                        subscription_delays.append(delay_minutes)
                
                avg_sub_delay = sum(subscription_delays) / len(subscription_delays) if subscription_delays else 0
                
                logger.info(f"""
📋 БУФЕР ЗАДАЧ (1500):
   📊 Всего в буфере: {buffer_size}/{self.max_buffer_size}
   👀 Просмотров: {view_count}
   📺 Подписок: {sub_count}
   ✅ Готовых к выполнению: {ready_count}
   ⏳ Будущих: {future_count}
   🔄 Актуализация: {'В процессе' if self.buffer_updating else 'Готов'}
   ⏰ Средняя задержка подписок: {avg_sub_delay:.1f} мин
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
📊 СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ (5 мин):
   ✅ Выполнено задач: {self.processed_tasks} ({tasks_per_min:.1f}/мин)
   📋 В буфере: {len(self.task_buffer)}/{self.max_buffer_size}
   📦 В Redis: {total_in_redis}
   🔄 Retry: {retry_count}
   🧠 Сессий: {session_stats['connected']}/{session_stats['total_loaded']}
   🚀 Производительность: {performance_status}
   ⚙️ Актуализация: {'Активна' if self.buffer_updating else 'Готова'}
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
                'last_buffer_load': self.last_buffer_load,
                'buffer_updating': self.buffer_updating
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
                # Очищаем буфер от задач с неактивными сессиями
                asyncio.create_task(self._cleanup_buffer_dead_sessions())
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
        async with self.buffer_lock:
            cleared_count = len(self.task_buffer)
            self.task_buffer.clear()
            logger.info(f"✅ Очищено {cleared_count} задач из буфера")
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы улучшенного воркера...")
        
        try:
            # Ждем завершения актуализации буфера
            while self.buffer_updating:
                await asyncio.sleep(0.1)
            
            # Сохраняем задачи из буфера обратно в Redis перед завершением
            if self.task_buffer:
                logger.info(f"💾 Сохранение {len(self.task_buffer)} задач из буфера в Redis...")
                
                tasks_data = {}
                for task in self.task_buffer:
                    try:
                        # Удаляем временные поля перед сохранением
                        clean_task = {k: v for k, v in task.items() if k != 'score'}
                        task_json = json.dumps(clean_task)
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
            
            # Закрываем БД
            await shutdown_db_pool()
            
            logger.info("✅ Улучшенный воркер корректно завершен")
            
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