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

class EnhancedTaskWorker:
    def __init__(self):
        self.redis_client = None
        self.running = False
        self.max_retries = 3
        
        # Параметры параллелизма (теперь настраиваемые)
        self.parallel_limits = {
            'view': 100,       # Будет обновляться из настроек
            'subscribe': 10     # Фиксированное значение
        }
        
        # Задержки между задачами
        self.task_delays = {
            'view': (1, 5),      # 1-5 секунд между просмотрами
            'subscribe': (15, 45) # 15-45 секунд между подписками
        }
        
        # Кэш настроек с новыми параметрами батчей
        self.cached_settings = {}
        self.last_settings_update = 0
        
        # Расширенные счетчики для статистики (ОБНОВЛЕНО)
        current_time = time.time()
        self.performance_stats = {
            'processed_tasks': 0,
            'success_count': 0,
            'error_count': 0,
            
            # Временная статистика (ОБНОВЛЕНО)
            'tasks_last_minute': deque(maxlen=60),  # Задачи за каждую секунду
            'tasks_last_hour': deque(maxlen=60),    # Задачи за каждую минуту
            'tasks_per_second': deque(maxlen=60),   # НОВОЕ - задачи за каждую секунду
            'last_minute_update': current_time,
            'last_hour_update': current_time,
            'last_second_update': current_time,     # НОВОЕ
            
            # Счетчики для вычисления средних (НОВОЕ)
            'total_tasks_executed': 0,              # НОВОЕ - общий счетчик выполненных задач
            'start_time': current_time,             # НОВОЕ - время запуска воркера
            'last_stats_save': current_time,        # НОВОЕ - время последнего сохранения статистики
            'tasks_this_period': 0,                 # НОВОЕ - задачи за текущий период
            
            # Батч статистика
            'batches_processed': 0,
            'avg_batch_size': 0,
            'last_batch_times': deque(maxlen=10),
            
            # Счетчики по типам задач
            'view_tasks_executed': 0,
            'subscribe_tasks_executed': 0,
            'total_execution_time': 0
        }
        
    async def start(self):
        logger.info("🚀 Запуск Enhanced Task Worker с исправленной статистикой...")
        
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
            
            # Запуск основного цикла
            self.running = True
            await self._main_loop()
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка воркера: {e}")
            raise
        finally:
            await self._shutdown()
    
    async def _update_cached_settings(self):
        """Обновляет кэшированные настройки включая новые батч параметры"""
        try:
            self.cached_settings = {
                'view_period': read_setting('followPeriod.txt', 3.0) * 3600,
                'view_delay': read_setting('delay.txt', 20.0) * 60,
                'sub_lag': read_setting('lag.txt', 14.0) * 60,
                'sub_range': read_setting('range.txt', 5.0) * 60,
                'timeout_count': int(read_setting('timeout_count.txt', 3.0)),
                'timeout_duration': read_setting('timeout_duration.txt', 13.0) * 60,
                
                # Новые настройки батчей
                'view_batch_size': int(read_setting('view_batch_size.txt', 100.0)),
                'view_batch_delay': read_setting('view_batch_delay.txt', 30.0),
                'subscribe_batch_delay': read_setting('subscribe_batch_delay.txt', 60.0),
                'accounts_delay': read_setting('accounts_delay.txt', 2.0) * 60
            }
            
            # Обновляем лимиты параллелизма
            self.parallel_limits['view'] = self.cached_settings['view_batch_size']
            
            self.last_settings_update = time.time()
            
            logger.info(f"""
⚙️ НАСТРОЙКИ ENHANCED ВОРКЕРА ОБНОВЛЕНЫ:
   👀 Период просмотров: {self.cached_settings['view_period']/3600:.1f} часов
   📦 Размер батча просмотров: {self.cached_settings['view_batch_size']}
   ⏳ Задержка батча просмотров: {self.cached_settings['view_batch_delay']} сек
   ⏳ Задержка батча подписок: {self.cached_settings['subscribe_batch_delay']} сек
   📺 Базовая задержка подписок: {self.cached_settings['sub_lag']/60:.1f} мин
   🎲 Разброс подписок: {self.cached_settings['sub_range']/60:.1f} мин
   🔢 Подписок до паузы: {self.cached_settings['timeout_count']}
   ⏸️ Длительность паузы: {self.cached_settings['timeout_duration']/60:.1f} мин
   ⏰ Задержка аккаунтов: {self.cached_settings['accounts_delay']/60:.1f} мин
            """)
            
        except Exception as e:
            logger.error(f"Ошибка обновления настроек: {e}")
    
    async def _save_stats_to_redis(self):
        """Сохраняет статистику воркера в Redis для админ панели"""
        try:
            current_time = time.time()
            
            # Вычисляем средние показатели
            uptime = current_time - self.performance_stats['start_time']
            total_executed = self.performance_stats['total_tasks_executed']
            
            avg_per_minute = (total_executed / (uptime / 60)) if uptime > 60 else 0
            avg_per_second = (total_executed / uptime) if uptime > 0 else 0
            
            # Статистика за последние периоды
            tasks_last_minute = sum(self.performance_stats['tasks_per_second'])
            tasks_last_hour = sum(self.performance_stats['tasks_last_minute'])
            tasks_last_5min = self.performance_stats['tasks_this_period']  # За последние 5 минут
            
            # Успешность выполнения
            success_rate = 0.0
            if self.performance_stats['processed_tasks'] > 0:
                success_rate = (self.performance_stats['success_count'] / self.performance_stats['processed_tasks']) * 100
            
            stats_data = {
                'timestamp': current_time,
                'tasks_last_minute': tasks_last_minute,
                'tasks_last_5min': tasks_last_5min,
                'tasks_last_hour': tasks_last_hour,
                'avg_tasks_per_minute': avg_per_minute,
                'avg_tasks_per_second': avg_per_second,
                'total_executed': total_executed,
                'success_rate': success_rate,
                'view_tasks': self.performance_stats['view_tasks_executed'],
                'subscribe_tasks': self.performance_stats['subscribe_tasks_executed'],
                'batches_processed': self.performance_stats['batches_processed'],
                'uptime': uptime,
                'error_count': self.performance_stats['error_count']
            }
            
            # Сохраняем в Redis с TTL 10 минут
            self.redis_client.setex(
                'worker_stats', 
                600,  # 10 минут TTL
                json.dumps(stats_data)
            )
            
            logger.debug(f"📊 Статистика сохранена в Redis: {tasks_last_minute} задач/мин, успешность {success_rate:.1f}%")
            
            # Сбрасываем счетчик текущего периода
            self.performance_stats['tasks_this_period'] = 0
            
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики в Redis: {e}")
    
    def _update_time_based_stats(self):
        """Обновляет временную статистику (ОБНОВЛЕНО)"""
        current_time = time.time()
        
        # Обновляем статистику по секундам (ОБНОВЛЕНО)
        if current_time - self.performance_stats['last_second_update'] >= 1.0:
            # Добавляем количество задач за последнюю секунду
            # Используем успешно выполненные задачи за последнюю секунду
            tasks_this_second = 0  # Будет обновляться в _execute_tasks_parallel
            
            self.performance_stats['tasks_per_second'].append(tasks_this_second)
            self.performance_stats['last_second_update'] = current_time
        
        # Обновляем статистику по минутам (ОБНОВЛЕНО)
        if current_time - self.performance_stats['last_minute_update'] >= 60.0:
            # Суммируем задачи за последнюю минуту
            tasks_this_minute = sum(self.performance_stats['tasks_per_second'])
            self.performance_stats['tasks_last_minute'].append(tasks_this_minute)
            self.performance_stats['last_minute_update'] = current_time
            
            # Логируем статистику за минуту
            logger.debug(f"📊 За последнюю минуту выполнено {tasks_this_minute} задач")
            
            # Очищаем счетчик секунд для новой минуты
            self.performance_stats['tasks_per_second'] = deque(maxlen=60)
        
        # Обновляем статистику по часам
        if current_time - self.performance_stats['last_hour_update'] >= 3600.0:
            # Суммируем задачи за час
            tasks_this_hour = sum(self.performance_stats['tasks_last_minute'])
            self.performance_stats['tasks_last_hour'].append(tasks_this_hour)
            self.performance_stats['last_hour_update'] = current_time
            
            logger.info(f"📊 За последний час выполнено {tasks_this_hour} задач")
    
    async def _main_loop(self):
        logger.info("🔄 Запуск основного цикла с батчевой обработкой и статистикой")
        
        last_stats_time = time.time()
        last_ban_check = time.time()
        last_cleanup = time.time()
        last_stats_save = time.time()  # НОВОЕ - для сохранения статистики
        cycle_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                current_time = time.time()
                
                # Обновляем временную статистику
                self._update_time_based_stats()
                
                # Проверяем команды от бота
                await self._process_worker_commands()
                
                # Обновляем настройки каждые 5 минут
                if current_time - self.last_settings_update > 300:
                    await self._update_cached_settings()
                
                # Сохраняем статистику каждую минуту (НОВОЕ)
                if current_time - last_stats_save > 60:
                    await self._save_stats_to_redis()
                    last_stats_save = current_time
                
                # Проверка забаненных аккаунтов (раз в час)
                if current_time - last_ban_check > 3600:
                    await self._check_banned_accounts_for_retry()
                    last_ban_check = current_time
                
                # Очистка старых задач (раз в 6 часов)
                if current_time - last_cleanup > 21600:
                    await self._cleanup_old_tasks()
                    last_cleanup = current_time
                
                # Основная обработка задач с батчами
                processed_in_cycle = await self._process_ready_tasks_with_batches()
                
                # Обрабатываем retry задачи
                await self._process_retry_tasks()
                
                # Статистика каждые 5 минут
                if current_time - last_stats_time > 300:
                    await self._log_enhanced_performance_stats()
                    self._reset_performance_counters()
                    last_stats_time = current_time
                
                # Адаптивная пауза
                if processed_in_cycle > 0:
                    pause_time = random.uniform(10, 20)
                else:
                    pause_time = random.uniform(30, 60)
                    
                # Логируем статус каждые 50 циклов
                if cycle_count % 50 == 0:
                    queue_size = self.redis_client.zcard("task_queue") or 0
                    ready_count = self.redis_client.zcount("task_queue", 0, current_time) or 0
                    logger.info(f"💓 Цикл #{cycle_count} | Очередь: {queue_size} | Готовых: {ready_count} | Выполнено: {self.performance_stats['total_tasks_executed']}")
                    
                await asyncio.sleep(pause_time)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в главном цикле: {e}")
                await asyncio.sleep(10)
    
    async def _process_ready_tasks_with_batches(self) -> int:
        """Обрабатывает готовые задачи пакетами с настраиваемыми задержками"""
        current_time = time.time()
        total_processed = 0
        
        try:
            # Получаем готовые задачи по типам
            view_tasks = await self._get_ready_tasks_by_type('view', current_time)
            subscribe_tasks = await self._get_ready_tasks_by_type('subscribe', current_time)
            
            # Обрабатываем просмотры батчами
            if view_tasks:
                batch_start_time = time.time()
                logger.info(f"👀 Обрабатываю батч из {len(view_tasks)} задач просмотра")
                
                view_results = await self._execute_tasks_parallel(view_tasks, 'view')
                total_processed += len(view_results)
                
                # Записываем время батча
                batch_duration = time.time() - batch_start_time
                self.performance_stats['last_batch_times'].append(batch_duration)
                self.performance_stats['batches_processed'] += 1
                self.performance_stats['total_execution_time'] += batch_duration
                
                # Настраиваемая задержка между батчами просмотров
                batch_delay = self.cached_settings['view_batch_delay']
                if len(view_tasks) > 50:  # Увеличиваем задержку для больших батчей
                    batch_delay *= 1.5
                    
                logger.info(f"⏳ Задержка между батчами просмотров: {batch_delay:.1f} сек")
                await asyncio.sleep(batch_delay)
            
            # Обрабатываем подписки (с фиксированным размером батча 5)
            if subscribe_tasks:
                # Ограничиваем размер батча подписок до 5
                subscribe_batch = subscribe_tasks[:5]
                
                batch_start_time = time.time()
                logger.info(f"📺 Обрабатываю батч из {len(subscribe_batch)} задач подписки")
                
                sub_results = await self._execute_tasks_parallel(subscribe_batch, 'subscribe')
                total_processed += len(sub_results)
                
                # Возвращаем необработанные задачи обратно в очередь
                if len(subscribe_tasks) > 5:
                    remaining_tasks = subscribe_tasks[5:]
                    await self._return_tasks_to_queue(remaining_tasks)
                    logger.info(f"🔄 Возвращено {len(remaining_tasks)} задач подписки в очередь")
                
                # Настраиваемая задержка между батчами подписок
                batch_delay = self.cached_settings['subscribe_batch_delay']
                logger.info(f"⏳ Задержка между батчами подписок: {batch_delay:.1f} сек")
                await asyncio.sleep(batch_delay)
            
            return total_processed
            
        except Exception as e:
            logger.error(f"Ошибка обработки готовых задач с батчами: {e}")
            return 0
    
    async def _return_tasks_to_queue(self, tasks: List[Dict]):
        """Возвращает задачи обратно в очередь Redis"""
        try:
            tasks_data = {}
            current_time = time.time()
            
            for task in tasks:
                # Добавляем небольшую задержку чтобы задачи не выполнились сразу
                new_execute_at = current_time + random.uniform(60, 300)  # 1-5 минут
                task['execute_at'] = new_execute_at
                
                task_json = json.dumps(task)
                tasks_data[task_json] = new_execute_at
            
            if tasks_data:
                self.redis_client.zadd("task_queue", tasks_data)
                logger.debug(f"🔄 Возвращено {len(tasks)} задач в очередь")
                
        except Exception as e:
            logger.error(f"Ошибка возврата задач в очередь: {e}")
    
    async def _get_ready_tasks_by_type(self, task_type: str, current_time: float) -> List[Dict]:
        """Получает готовые задачи определенного типа с учетом батч размера"""
        try:
            if task_type == 'view':
                limit = self.cached_settings['view_batch_size']
            elif task_type == 'subscribe':
                limit = 10  # Фиксированный лимит для подписок, но батч будет 5
            else:
                limit = 50
            
            # Получаем готовые задачи из Redis
            ready_tasks_data = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max=current_time,
                withscores=True,
                start=0,
                num=limit * 2  # Берем с запасом для фильтрации
            )
            
            if not ready_tasks_data:
                return []
            
            # Фильтруем по типу задач
            filtered_tasks = []
            removed_broken_tasks = 0
            
            for task_json, score in ready_tasks_data:
                try:
                    task_data = json.loads(task_json)
                    if task_data.get('task_type') == task_type:
                        task_data['redis_key'] = task_json
                        filtered_tasks.append(task_data)
                        
                        if len(filtered_tasks) >= limit:
                            break
                            
                except Exception as e:
                    logger.error(f"Ошибка парсинга задачи: {e}")
                    # Удаляем битую задачу
                    self.redis_client.zrem("task_queue", task_json)
                    removed_broken_tasks += 1
            
            if removed_broken_tasks > 0:
                logger.warning(f"🗑️ Удалено {removed_broken_tasks} битых задач из очереди")
            
            # Удаляем взятые задачи из Redis
            for task in filtered_tasks:
                self.redis_client.zrem("task_queue", task['redis_key'])
                del task['redis_key']
            
            return filtered_tasks
            
        except Exception as e:
            logger.error(f"Ошибка получения задач типа {task_type}: {e}")
            return []
    
    async def _execute_tasks_parallel(self, tasks: List[Dict], task_type: str) -> List[bool]:
        """Выполняет список задач параллельно (ОБНОВЛЕНО для статистики)"""
        if not tasks:
            return []
        
        try:
            # Создаем параллельные задачи с разбросом времени запуска
            parallel_tasks = []
            delay_range = self.task_delays[task_type]
            
            for i, task in enumerate(tasks):
                # Задержка старта для разброса нагрузки
                start_delay = random.uniform(*delay_range) * (i / len(tasks))
                
                parallel_task = asyncio.create_task(
                    self._execute_single_task_with_delay(task, start_delay)
                )
                parallel_tasks.append(parallel_task)
            
            # Выполняем все задачи параллельно
            results = await asyncio.gather(*parallel_tasks, return_exceptions=True)
            
            # Анализируем результаты
            success_count = 0
            error_count = 0
            flood_wait_count = 0
            
            for i, result in enumerate(results):
                task = tasks[i]
                phone = task.get('phone', 'unknown')
                channel = task.get('channel', 'unknown')
                
                if isinstance(result, Exception):
                    error_count += 1
                    if 'FloodWait' in str(result):
                        flood_wait_count += 1
                    logger.error(f"💥 {task_type} | {phone} | @{channel} | {result}")
                elif result:
                    success_count += 1
                    logger.debug(f"✅ {task_type} | {phone} | @{channel}")
                else:
                    error_count += 1
                    logger.warning(f"❌ {task_type} | {phone} | @{channel}")
            
            # Обновляем счетчики (ОБНОВЛЕНО)
            self.performance_stats['processed_tasks'] += len(results)
            self.performance_stats['success_count'] += success_count
            self.performance_stats['error_count'] += error_count
            
            # НОВОЕ: Обновляем общий счетчик и счетчики по типам
            self.performance_stats['total_tasks_executed'] += success_count
            self.performance_stats['tasks_this_period'] += success_count
            
            if task_type == 'view':
                self.performance_stats['view_tasks_executed'] += success_count
            elif task_type == 'subscribe':
                self.performance_stats['subscribe_tasks_executed'] += success_count
            
            # Обновляем средний размер батча
            if self.performance_stats['batches_processed'] > 0:
                self.performance_stats['avg_batch_size'] = (
                    (self.performance_stats['avg_batch_size'] * (self.performance_stats['batches_processed'] - 1) + len(tasks)) 
                    / self.performance_stats['batches_processed']
                )
            
            success_rate = (success_count / len(results)) * 100 if results else 0
            
            # Дополнительная информация для подписок
            if task_type == 'subscribe' and results:
                logger.info(f"📊 {task_type.upper()} БАТЧ: {success_count}/{len(results)} успешно ({success_rate:.1f}%) | FloodWait: {flood_wait_count}")
            else:
                logger.info(f"📊 {task_type.upper()} БАТЧ: {success_count}/{len(results)} успешно ({success_rate:.1f}%)")
            
            return [r for r in results if not isinstance(r, Exception)]
            
        except Exception as e:
            logger.error(f"Ошибка параллельного выполнения {task_type}: {e}")
            return []
    
    async def _execute_single_task_with_delay(self, task: Dict, delay: float) -> bool:
        """Выполняет одну задачу с начальной задержкой"""
        try:
            # Задержка для разброса нагрузки
            if delay > 0:
                await asyncio.sleep(delay)
            
            # Выполняем задачу
            return await self._execute_task_simple(task)
            
        except Exception as e:
            logger.error(f"Ошибка выполнения задачи с задержкой: {e}")
            return False
    
    async def _execute_task_simple(self, task: Dict) -> bool:
        """Выполняет отдельную задачу"""
        task_type = task.get('task_type', '')
        session_data = task.get('account_session', '')
        phone = task.get('phone', 'unknown')
        channel = task.get('channel', 'unknown')
        
        if not session_data:
            logger.warning(f"❌ {phone}: нет session_data")
            return False
        
        client = None
        
        try:
            # 1. Создаем клиент "на лету"
            client = TelegramClient(
                StringSession(session_data),
                API_ID, API_HASH,
                lang_code=find_lang_code(task.get('lang', 'English')),
                connection_retries=1,
                timeout=20
            )
            
            # 2. Подключаемся
            await client.connect()
            
            # 3. Проверяем авторизацию
            if not await client.is_user_authorized():
                logger.warning(f"❌ {phone}: не авторизован")
                await self._handle_task_failure(phone, task_type)
                return False
            
            # 4. Выполняем задачу в зависимости от типа
            if task_type == 'view':
                success = await self._execute_view_task(client, task)
            elif task_type == 'subscribe':
                success = await self._execute_subscribe_task(client, task)
            else:
                logger.warning(f"❌ {phone}: неизвестный тип задачи {task_type}")
                return False
            
            # 5. Обрабатываем результат
            if success:
                await self._handle_task_success(phone)
                return True
            else:
                await self._handle_task_failure(phone, task_type)
                return False
                
        except FloodWaitError as e:
            logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s")
            await self._add_to_retry_queue(task, delay=e.seconds)
            return False
            
        except (RPCError, AuthKeyInvalidError) as e:
            logger.warning(f"❌ {phone}: критическая ошибка - {e}")
            await self._handle_task_failure(phone, task_type)
            return False
            
        except Exception as e:
            logger.error(f"💥 {phone}: неожиданная ошибка - {e}")
            await self._handle_task_failure(phone, task_type)
            return False
            
        finally:
            # 6. ВСЕГДА отключаемся
            if client:
                try:
                    await client.disconnect()
                except Exception as e:
                    logger.debug(f"Ошибка отключения клиента {phone}: {e}")
    
    async def _execute_view_task(self, client: TelegramClient, task: Dict) -> bool:
        """Выполняет задачу просмотра"""
        channel = task.get('channel', '')
        post_id = task.get('post_id', 0)
        phone = task.get('phone', 'unknown')
        
        try:
            # Получаем entity канала
            channel_entity = await client.get_entity(channel)
            
            # Выполняем просмотр
            await client(GetMessagesViewsRequest(
                peer=channel_entity,
                id=[post_id],
                increment=True
            ))
            
            # Имитируем время чтения
            reading_time = random.uniform(3, 7)
            await asyncio.sleep(reading_time)
            
            logger.debug(f"✅ {phone}: просмотр поста {post_id} в @{channel}")
            return True
            
        except Exception as e:
            logger.warning(f"❌ {phone}: ошибка просмотра @{channel}:{post_id} - {e}")
            return False
    
    async def _execute_subscribe_task(self, client: TelegramClient, task: Dict) -> bool:
        """Выполняет задачу подписки"""
        channel = task.get('channel', '')
        phone = task.get('phone', 'unknown')
        
        try:
            # Получаем entity канала
            channel_entity = await client.get_entity(channel)
            
            # Логируем информацию о времени выполнения
            created_at = task.get('created_at', 0)
            execute_at = task.get('execute_at', 0)
            current_time = time.time()
            
            if created_at and execute_at:
                planned_delay = (execute_at - created_at) / 60
                actual_delay = (current_time - created_at) / 60
                execution_drift = (current_time - execute_at) / 60
                
                if abs(execution_drift) <= 2.0:
                    timing_status = "⏰ ТОЧНО"
                elif execution_drift < 0:
                    timing_status = f"🚀 РАНО ({abs(execution_drift):.1f}мин)"
                else:
                    timing_status = f"⏳ ПОЗДНО (+{execution_drift:.1f}мин)"
                
                logger.info(f"📺 {phone} → @{channel} | Планировалось: {planned_delay:.1f}мин | Факт: {actual_delay:.1f}мин | {timing_status}")
            
            # Выполняем подписку
            await client(JoinChannelRequest(channel_entity))
            
            # Небольшая задержка после подписки
            await asyncio.sleep(random.uniform(2, 5))
            
            logger.info(f"✅ {phone}: подписан на @{channel}")
            return True
            
        except Exception as e:
            logger.warning(f"❌ {phone}: ошибка подписки на @{channel} - {e}")
            return False
    
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
            
            # Обрабатываем до 10 retry задач за раз
            for _ in range(10):
                task_data = self.redis_client.rpop('retry_tasks')
                if not task_data:
                    break
                
                try:
                    task = json.loads(task_data)
                    
                    # Проверяем время повтора
                    if task.get('retry_after', 0) <= current_time:
                        # Время пришло - выполняем
                        success = await self._execute_task_simple(task)
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
            elif command['command'] == 'get_stats':
                logger.info("📊 Запрос статистики от админ панели")
                await self._save_stats_to_redis()  # Принудительно сохраняем статистику
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
                    # Отмечаем попытку проверки
                    await mark_account_retry_attempt(phone)
                    
                    # Создаем тестовую задачу подписки
                    test_task = {
                        'account_session': account['session_data'],
                        'phone': phone,
                        'channel': 'telegram',  # Тестовый канал
                        'lang': account['lang'],
                        'task_type': 'subscribe'
                    }
                    
                    # Пытаемся выполнить тестовую задачу
                    success = await self._execute_task_simple(test_task)
                    
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
    
    def _reset_performance_counters(self):
        """Сбрасывает счетчики производительности за период"""
        self.performance_stats['processed_tasks'] = 0
        self.performance_stats['success_count'] = 0
        self.performance_stats['error_count'] = 0
        # НЕ сбрасываем общие счетчики total_tasks_executed и счетчики по типам
    
    async def _log_enhanced_performance_stats(self):
        """Логирует расширенную статистику производительности"""
        try:
            # Статистика Redis очереди
            total_in_redis = self.redis_client.zcard("task_queue") or 0
            retry_count = self.redis_client.llen("retry_tasks") or 0
            current_time = time.time()
            ready_in_redis = self.redis_client.zcount("task_queue", 0, current_time) or 0
            
            # Статистика на будущее
            ready_next_minute = self.redis_client.zcount("task_queue", current_time, current_time + 60) or 0
            ready_next_hour = self.redis_client.zcount("task_queue", current_time, current_time + 3600) or 0
            
            # Производительность
            total_processed = self.performance_stats['processed_tasks']
            tasks_per_min = total_processed / 5 if total_processed > 0 else 0
            success_rate = (self.performance_stats['success_count'] / total_processed * 100) if total_processed > 0 else 0
            
            # Батч статистика
            avg_batch_time = sum(self.performance_stats['last_batch_times']) / len(self.performance_stats['last_batch_times']) if self.performance_stats['last_batch_times'] else 0
            batches_processed = self.performance_stats['batches_processed']
            avg_batch_size = self.performance_stats['avg_batch_size']
            
            # Временная статистика
            tasks_last_minute_total = sum(self.performance_stats['tasks_per_second'])
            tasks_last_hour_total = sum(self.performance_stats['tasks_last_minute'])
            
            # Статистика по типам задач
            view_tasks = self.performance_stats['view_tasks_executed']
            subscribe_tasks = self.performance_stats['subscribe_tasks_executed']
            total_execution_time = self.performance_stats['total_execution_time']
            
            # Общая статистика
            uptime = current_time - self.performance_stats['start_time']
            total_executed = self.performance_stats['total_tasks_executed']
            avg_per_minute = (total_executed / (uptime / 60)) if uptime > 60 else 0
            
            # Статус производительности
            if tasks_per_min > 15 and success_rate > 80:
                performance_status = "🚀 ОТЛИЧНО"
            elif tasks_per_min > 8 and success_rate > 70:
                performance_status = "✅ ХОРОШО"
            elif tasks_per_min > 3 and success_rate > 50:
                performance_status = "⚠️ СРЕДНЕ"
            else:
                performance_status = "❌ НИЗКО"
            
            logger.info(f"""
📊 РАСШИРЕННАЯ СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ (5 мин):
   
   🎯 ВЫПОЛНЕНИЕ ЗА ПЕРИОД:
   ✅ Выполнено задач: {total_processed} ({tasks_per_min:.1f}/мин)
   📈 Успешность: {success_rate:.1f}% ({self.performance_stats['success_count']}/{total_processed})
   ❌ Ошибок: {self.performance_stats['error_count']}
   
   🎯 ОБЩАЯ СТАТИСТИКА:
   🏆 Всего выполнено: {total_executed} задач
   ⏰ Время работы: {uptime/3600:.1f} часов
   📈 Средняя производительность: {avg_per_minute:.1f} задач/мин
   
   📦 БАТЧИ:
   📊 Обработано батчей: {batches_processed}
   📏 Средний размер батча: {avg_batch_size:.1f}
   ⏱️ Среднее время батча: {avg_batch_time:.1f}с
   🕐 Общее время выполнения: {total_execution_time:.1f}с
   
   📈 ПО ТИПАМ ЗАДАЧ:
   👀 Всего просмотров: {view_tasks}
   📺 Всего подписок: {subscribe_tasks}
   
   📈 ВРЕМЕННАЯ СТАТИСТИКА:
   ⏰ Задач за текущую минуту: {tasks_last_minute_total}
   🕐 Задач за текущий час: {tasks_last_hour_total}
   
   📦 ОЧЕРЕДЬ REDIS:
   📋 Всего задач: {total_in_redis}
   ✅ Готовых сейчас: {ready_in_redis}
   ⏰ Готовых за минуту: {ready_next_minute}
   🕐 Готовых за час: {ready_next_hour}
   🔄 Retry задач: {retry_count}
   
   🚀 ОБЩИЙ СТАТУС: {performance_status}""")
                
        except Exception as e:
            logger.error(f"Ошибка логирования расширенной статистики: {e}")
    
    async def get_enhanced_task_stats(self) -> Dict:
        """Возвращает расширенную статистику задач для внешнего использования"""
        try:
            current_time = time.time()
            
            total_tasks = self.redis_client.zcard("task_queue") or 0
            ready_tasks = self.redis_client.zcount("task_queue", 0, current_time) or 0
            retry_tasks = self.redis_client.llen("retry_tasks") or 0
            
            # Готовые задачи на ближайшие периоды
            ready_next_minute = self.redis_client.zcount("task_queue", current_time, current_time + 60) or 0
            ready_next_hour = self.redis_client.zcount("task_queue", current_time, current_time + 3600) or 0
            
            # Временная статистика
            tasks_last_minute = sum(self.performance_stats['tasks_per_second'])
            tasks_last_hour = sum(self.performance_stats['tasks_last_minute'])
            
            # Батч статистика
            avg_batch_time = sum(self.performance_stats['last_batch_times']) / len(self.performance_stats['last_batch_times']) if self.performance_stats['last_batch_times'] else 0
            
            # Общая статистика
            uptime = current_time - self.performance_stats['start_time']
            total_executed = self.performance_stats['total_tasks_executed']
            avg_per_minute = (total_executed / (uptime / 60)) if uptime > 60 else 0
            avg_per_second = (total_executed / uptime) if uptime > 0 else 0
            
            return {
                'total_tasks': total_tasks,
                'ready_tasks': ready_tasks,
                'future_tasks': total_tasks - ready_tasks,
                'retry_tasks': retry_tasks,
                'processed_last_period': self.performance_stats['processed_tasks'],
                'success_rate': (self.performance_stats['success_count'] / max(self.performance_stats['processed_tasks'], 1)) * 100,
                
                # Расширенные метрики
                'ready_tasks_minute': ready_next_minute,
                'ready_tasks_hour': ready_next_hour,
                'tasks_last_minute': tasks_last_minute,
                'tasks_last_5min': self.performance_stats['tasks_this_period'],
                'tasks_last_hour': tasks_last_hour,
                
                # Средние показатели
                'avg_tasks_per_minute': avg_per_minute,
                'avg_tasks_per_second': avg_per_second,
                
                # Батч метрики
                'batches_processed': self.performance_stats['batches_processed'],
                'avg_batch_size': self.performance_stats['avg_batch_size'],
                'avg_batch_time': avg_batch_time,
                
                # Задачи по типам
                'view_tasks_executed': self.performance_stats['view_tasks_executed'],
                'subscribe_tasks_executed': self.performance_stats['subscribe_tasks_executed'],
                'total_tasks_executed': total_executed,
                
                # Настройки
                'view_batch_size': self.cached_settings.get('view_batch_size', 100),
                'view_batch_delay': self.cached_settings.get('view_batch_delay', 30),
                'subscribe_batch_delay': self.cached_settings.get('subscribe_batch_delay', 60),
                
                # Время
                'timestamp': current_time,
                'uptime': uptime
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения расширенной статистики задач: {e}")
            return {}
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка enhanced воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы enhanced воркера...")
        
        try:
            # Сохраняем финальную статистику
            await self._save_stats_to_redis()
            
            # Логируем финальную статистику
            total_uptime = time.time() - self.performance_stats['start_time']
            total_processed = self.performance_stats['total_tasks_executed']
            
            logger.info(f"""
📊 ФИНАЛЬНАЯ СТАТИСТИКА ВОРКЕРА:
   ⏰ Время работы: {total_uptime/3600:.1f} часов
   ✅ Всего выполнено задач: {total_processed}
   👀 Просмотров: {self.performance_stats['view_tasks_executed']}
   📺 Подписок: {self.performance_stats['subscribe_tasks_executed']}
   📦 Батчей обработано: {self.performance_stats['batches_processed']}
   🚀 Средняя производительность: {total_processed/(total_uptime/3600):.1f} задач/час
            """)
            
            # Закрываем Redis
            if self.redis_client:
                self.redis_client.close()
            
            # Закрываем БД
            await shutdown_db_pool()
            
            logger.info("✅ Enhanced воркер корректно завершен")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении: {e}")

# Алиас для обратной совместимости
SimpleTaskWorker = EnhancedTaskWorker

# Запуск воркера
async def main():
    """Главная функция воркера"""
    worker = EnhancedTaskWorker()
    
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
        logger.info("✅ uvloop установлен для максимальной производительности")
    except ImportError:
        logger.info("⚠️ uvloop недоступен, используем стандартный event loop")
    
    # Запускаем воркер
    logger.info("🚀 Запуск Enhanced Task Worker с исправленной статистикой...")
    asyncio.run(main())