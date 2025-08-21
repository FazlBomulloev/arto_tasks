import asyncio
import logging
import time
import json
import random
from typing import Dict, List, Optional
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

class SimpleTaskWorker:
    def __init__(self):
        self.redis_client = None
        self.running = False
        self.max_retries = 3
        
        # Параметры параллелизма
        self.parallel_limits = {
            'view':100,       
            'subscribe': 10     
        }
        
        # Задержки между задачами
        self.task_delays = {
            'view': (1, 5),      # 1-5 секунд между просмотрами
            'subscribe': (15, 45) # 15-45 секунд между подписками
        }
        
        # Счетчики производительности
        self.processed_tasks = 0
        self.success_count = 0
        self.error_count = 0
        
        # Кэш настроек
        self.cached_settings = {}
        self.last_settings_update = 0
        
    async def start(self):
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
        """Обновляет кэшированные настройки"""
        try:
            self.cached_settings = {
                'view_period': read_setting('followPeriod.txt', 3.0) * 3600,
                'view_delay': read_setting('delay.txt', 20.0) * 60,
                'sub_lag': read_setting('lag.txt', 14.0) * 60,
                'sub_range': read_setting('range.txt', 5.0) * 60,
                'timeout_count': int(read_setting('timeout_count.txt', 3.0)),
                'timeout_duration': read_setting('timeout_duration.txt', 13.0) * 60
            }
            self.last_settings_update = time.time()
            
            logger.info(f"""
⚙️ НАСТРОЙКИ ВОРКЕРА ОБНОВЛЕНЫ:
   👀 Период просмотров: {self.cached_settings['view_period']/3600:.1f} часов
   📺 Базовая задержка подписок: {self.cached_settings['sub_lag']/60:.1f} мин
   🎲 Разброс подписок: {self.cached_settings['sub_range']/60:.1f} мин
   🔢 Подписок до паузы: {self.cached_settings['timeout_count']}
   ⏸️ Длительность паузы: {self.cached_settings['timeout_duration']/60:.1f} мин
            """)
            
        except Exception as e:
            logger.error(f"Ошибка обновления настроек: {e}")
    
    async def _main_loop(self):
        logger.info("🔄 Запуск основного цикла обработки задач")
        
        last_stats_time = time.time()
        last_ban_check = time.time()
        cycle_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                
                # Проверяем команды от бота
                await self._process_worker_commands()
                
                # Обновляем настройки каждые 5 минут
                if time.time() - self.last_settings_update > 300:
                    await self._update_cached_settings()
                
                # Проверка забаненных аккаунтов (раз в час)
                if time.time() - last_ban_check > 3600:
                    await self._check_banned_accounts_for_retry()
                    last_ban_check = time.time()
                
                # Основная обработка задач
                processed_in_cycle = await self._process_ready_tasks()
                
                # Обрабатываем retry задачи
                await self._process_retry_tasks()
                
                # Статистика каждые 5 минут
                if time.time() - last_stats_time > 300:
                    await self._log_performance_stats()
                    self.processed_tasks = 0
                    self.success_count = 0
                    self.error_count = 0
                    last_stats_time = time.time()
                
                # Адаптивная пауза
                if processed_in_cycle > 0:
                    pause_time = random.uniform(10, 20)  # Короткая пауза если есть работа
                else:
                    pause_time = random.uniform(30, 60)  # Длинная пауза если нет задач
                    
                await asyncio.sleep(pause_time)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в главном цикле: {e}")
                await asyncio.sleep(10)
    
    async def _process_ready_tasks(self) -> int:
        """Обрабатывает готовые задачи параллельно по типам"""
        current_time = time.time()
        total_processed = 0
        
        try:
            # Получаем готовые задачи по типам
            view_tasks = await self._get_ready_tasks_by_type('view', current_time)
            subscribe_tasks = await self._get_ready_tasks_by_type('subscribe', current_time)
            
            # Обрабатываем просмотры (больше параллельности)
            if view_tasks:
                logger.info(f"👀 Обрабатываю {len(view_tasks)} задач просмотра")
                view_results = await self._execute_tasks_parallel(view_tasks, 'view')
                total_processed += len(view_results)
                
                # Короткая пауза между типами задач
                await asyncio.sleep(random.uniform(5, 10))
            
            # Обрабатываем подписки (меньше параллельности, больше осторожности)
            if subscribe_tasks:
                logger.info(f"📺 Обрабатываю {len(subscribe_tasks)} задач подписки")
                sub_results = await self._execute_tasks_parallel(subscribe_tasks, 'subscribe')
                total_processed += len(sub_results)
            
            return total_processed
            
        except Exception as e:
            logger.error(f"Ошибка обработки готовых задач: {e}")
            return 0
    
    async def _get_ready_tasks_by_type(self, task_type: str, current_time: float) -> List[Dict]:
        """Получает готовые задачи определенного типа из Redis"""
        try:
            limit = self.parallel_limits[task_type]
            
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
            for task_json, score in ready_tasks_data:
                try:
                    task_data = json.loads(task_json)
                    if task_data.get('task_type') == task_type:
                        task_data['redis_key'] = task_json  # Сохраняем ключ для удаления
                        filtered_tasks.append(task_data)
                        
                        if len(filtered_tasks) >= limit:
                            break
                            
                except Exception as e:
                    logger.error(f"Ошибка парсинга задачи: {e}")
                    # Удаляем битую задачу
                    self.redis_client.zrem("task_queue", task_json)
            
            # Удаляем взятые задачи из Redis
            for task in filtered_tasks:
                self.redis_client.zrem("task_queue", task['redis_key'])
                del task['redis_key']  # Убираем служебный ключ
            
            return filtered_tasks
            
        except Exception as e:
            logger.error(f"Ошибка получения задач типа {task_type}: {e}")
            return []
    
    async def _execute_tasks_parallel(self, tasks: List[Dict], task_type: str) -> List[bool]:
        """Выполняет список задач параллельно"""
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
            
            for i, result in enumerate(results):
                task = tasks[i]
                phone = task.get('phone', 'unknown')
                channel = task.get('channel', 'unknown')
                
                if isinstance(result, Exception):
                    error_count += 1
                    logger.error(f"💥 {task_type} | {phone} | @{channel} | {result}")
                elif result:
                    success_count += 1
                    logger.debug(f"✅ {task_type} | {phone} | @{channel}")
                else:
                    error_count += 1
                    logger.warning(f"❌ {task_type} | {phone} | @{channel}")
            
            # Обновляем общие счетчики
            self.processed_tasks += len(results)
            self.success_count += success_count
            self.error_count += error_count
            
            success_rate = (success_count / len(results)) * 100 if results else 0
            logger.info(f"📊 {task_type.upper()}: {success_count}/{len(results)} успешно ({success_rate:.1f}%)")
            
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
                
        except Exception as e:
            logger.error(f"Ошибка обработки команд: {e}")
    
    async def _check_banned_accounts_for_retry(self):
        """Проверяет забаненные аккаунты раз в 120 часов"""
        try:
            ban_accounts = await get_ban_accounts_for_retry()
            
            if not ban_accounts:
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
    
    async def _log_performance_stats(self):
        """Логирует статистику производительности"""
        try:
            # Статистика Redis очереди
            total_in_redis = self.redis_client.zcard("task_queue") or 0
            retry_count = self.redis_client.llen("retry_tasks") or 0
            current_time = time.time()
            ready_in_redis = self.redis_client.zcount("task_queue", 0, current_time) or 0
            
            # Производительность
            total_processed = self.processed_tasks
            tasks_per_min = total_processed / 5 if total_processed > 0 else 0
            success_rate = (self.success_count / total_processed * 100) if total_processed > 0 else 0
            
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
📊 СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ (5 мин):
   ✅ Выполнено задач: {total_processed} ({tasks_per_min:.1f}/мин)
   📈 Успешность: {success_rate:.1f}% ({self.success_count}/{total_processed})
   ❌ Ошибок: {self.error_count}
   📦 В Redis: {total_in_redis} (готовых: {ready_in_redis})
   🔄 Retry: {retry_count}
   🚀 Производительность: {performance_status}""")
                
        except Exception as e:
            logger.error(f"Ошибка логирования статистики: {e}")
    
    async def get_task_stats(self) -> Dict:
        """Возвращает статистику задач для внешнего использования"""
        try:
            current_time = time.time()
            
            total_tasks = self.redis_client.zcard("task_queue") or 0
            ready_tasks = self.redis_client.zcount("task_queue", 0, current_time) or 0
            retry_tasks = self.redis_client.llen("retry_tasks") or 0
            
            return {
                'total_tasks': total_tasks,
                'ready_tasks': ready_tasks,
                'future_tasks': total_tasks - ready_tasks,
                'retry_tasks': retry_tasks,
                'processed_last_period': self.processed_tasks,
                'success_rate': (self.success_count / max(self.processed_tasks, 1)) * 100
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики задач: {e}")
            return {}
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы упрощенного воркера...")
        
        try:
            # Закрываем Redis
            if self.redis_client:
                self.redis_client.close()
            
            # Закрываем БД
            await shutdown_db_pool()
            
            logger.info("✅ Упрощенный воркер корректно завершен")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении: {e}")

# Запуск воркера
async def main():
    """Главная функция воркера"""
    worker = SimpleTaskWorker()
    
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