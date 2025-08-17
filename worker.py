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

class ShardedTaskWorker:
    def __init__(self):
        self.redis_client = None
        self.running = False
        self.max_retries = 3
        self.sessions_loaded = False
        self.ban_retry_hours = 120  # 120 часов между попытками для забаненных
        
        # ✅ НАСТРОЙКИ ШАРДИНГА
        self.max_shards_per_cycle = 10  # Максимум шардов для проверки за раз
        self.max_tasks_per_shard = 50   # Максимум задач с одного шарда
        self.performance_mode = 'adaptive' 
        
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
            
            # Определяем режим производительности на основе нагрузки
            await self._detect_performance_mode()
            
            # Пытаемся загрузить сессии
            logger.info("🧠 Попытка предзагрузки сессий...")
            await self._try_preload_sessions()
            
            # Запуск основного цикла
            self.running = True
            await self._main_loop()
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка воркера: {e}")
            raise
        finally:
            await self._shutdown()
    
    async def _detect_performance_mode(self):
        """Определяет оптимальный режим производительности на основе нагрузки"""
        try:
            # Анализируем количество активных шардов
            view_shards = self.redis_client.zcard("active_view_shards") or 0
            sub_shards = self.redis_client.zcard("active_subscription_shards") or 0
            total_shards = view_shards + sub_shards
            
            # Адаптивная настройка режима
            if total_shards <= 5:
                self.performance_mode = 'conservative'
                self.max_shards_per_cycle = 5
                self.max_tasks_per_shard = 30
                mode_desc = "КОНСЕРВАТИВНЫЙ (малая нагрузка)"
            elif total_shards <= 20:
                self.performance_mode = 'adaptive'  
                self.max_shards_per_cycle = 10
                self.max_tasks_per_shard = 50
                mode_desc = "АДАПТИВНЫЙ (средняя нагрузка)"
            else:
                self.performance_mode = 'aggressive'
                self.max_shards_per_cycle = 15
                self.max_tasks_per_shard = 70
                mode_desc = "АГРЕССИВНЫЙ (высокая нагрузка)"
            
            logger.info(f"""
🎯 Режим производительности: {mode_desc}
   🗂️ Всего активных шардов: {total_shards}
   📊 Шардов за цикл: {self.max_shards_per_cycle}
   📋 Задач с шарда: {self.max_tasks_per_shard}
            """)
            
        except Exception as e:
            logger.error(f"Ошибка определения режима производительности: {e}")
            # Fallback на консервативный режим
            self.performance_mode = 'conservative'
    
    async def _try_preload_sessions(self):
        """Пытается загрузить сессии"""
        try:
            results = await global_session_manager.preload_all_sessions()
            
            if results['loaded'] == 0:
                logger.warning("⚠️ Нет активных аккаунтов для загрузки")
                logger.info("💡 Воркер будет ждать появления аккаунтов...")
                self.sessions_loaded = False
            else:
                logger.info(f"✅ Предзагрузка завершена: {results['loaded']} сессий готово к работе")
                self.sessions_loaded = True
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке сессий: {e}")
            logger.info("💡 Воркер продолжит работу в режиме ожидания аккаунтов...")
            self.sessions_loaded = False
    
    async def _main_loop(self):
        """Оптимизированный главный цикл с ШАРДИНГОМ"""
        logger.info("🔄 Запуск цикла обработки задач")
        
        # Счетчики для статистики
        processed_views = 0
        processed_subs = 0
        processed_shards = 0
        last_stats_time = time.time()
        last_session_check = time.time()
        last_ban_check = time.time()
        last_cleanup = time.time()
        last_performance_check = time.time()
        
        while self.running:
            try:
                # Проверяем команды от бота
                await self._process_worker_commands()
                
                # Проверяем и адаптируем режим производительности каждые 15 минут
                if time.time() - last_performance_check > 900:
                    await self._detect_performance_mode()
                    last_performance_check = time.time()
                
                # Если сессии не загружены, пытаемся загрузить каждые 5 минут
                if not self.sessions_loaded and time.time() - last_session_check > 300:
                    logger.info("🔍 Проверяю появление новых аккаунтов...")
                    await self._try_preload_sessions()
                    last_session_check = time.time()
                
                # Проверка забаненных аккаунтов (раз в час)
                if time.time() - last_ban_check > 3600:
                    await self._check_banned_accounts_for_retry()
                    last_ban_check = time.time()
                
                # Очистка истекших шардов (каждые 30 минут)
                if time.time() - last_cleanup > 1800:
                    await self._cleanup_expired_shards()
                    last_cleanup = time.time()
                
                # Если сессии загружены, обрабатываем задачи
                if self.sessions_loaded:
                    
                    # ✅ ПОЛУЧАЕМ ГОТОВЫЕ ЗАДАЧИ 
                    ready_tasks = await self._get_ready_tasks_from_shards()
                    view_tasks = ready_tasks['view_tasks']
                    sub_tasks = ready_tasks['subscription_tasks']
                    processed_shards += ready_tasks['shards_processed']
                    
                    # ✅ ПАРАЛЛЕЛЬНО обрабатываем разные типы задач
                    processing_tasks = []
                    
                    if view_tasks:
                        processing_tasks.append(
                            asyncio.create_task(
                                self._process_view_batch_optimized(view_tasks),
                                name="process_views"
                            )
                        )
                    
                    if sub_tasks:
                        processing_tasks.append(
                            asyncio.create_task(
                                self._process_subscription_batch_sequential(sub_tasks),
                                name="process_subscriptions"
                            )
                        )
                    
                    # Обрабатываем retry задачи
                    processing_tasks.append(
                        asyncio.create_task(
                            self._process_retry_tasks(),
                            name="process_retry"
                        )
                    )
                    
                    # Ждем завершения всех задач
                    if processing_tasks:
                        results = await asyncio.gather(*processing_tasks, return_exceptions=True)
                        
                        # Подсчитываем результаты
                        for i, result in enumerate(results):
                            if isinstance(result, int):
                                if processing_tasks[i].get_name() == "process_views":
                                    processed_views += result
                                elif processing_tasks[i].get_name() == "process_subscriptions":
                                    processed_subs += result
                            elif isinstance(result, Exception):
                                logger.error(f"Ошибка в задаче {processing_tasks[i].get_name()}: {result}")
                    
                    # Health check сессий
                    if random.random() < 0.005:  # ~0.5% шанс
                        health_stats = await global_session_manager.health_check()
                        if health_stats.get('removed_dead', 0) > 0:
                            logger.info(f"🔧 Очищено {health_stats['removed_dead']} мертвых сессий")
                
                # Статистика каждые 5 минут
                if time.time() - last_stats_time > 300:
                    await self._log_performance_stats(processed_views, processed_subs, processed_shards)
                    processed_views = processed_subs = processed_shards = 0
                    last_stats_time = time.time()
                
                # Адаптивная пауза в зависимости от режима
                sleep_time = {
                    'conservative': 3,
                    'adaptive': 2, 
                    'aggressive': 1
                }.get(self.performance_mode, 2)
                
                if not self.sessions_loaded:
                    sleep_time *= 10  # Больше пауза если нет сессий
                
                await asyncio.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Получен сигнал остановки")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в главном цикле: {e}")
                await asyncio.sleep(5)
    
    async def _get_ready_tasks_from_shards(self) -> Dict[str, List[Dict]]:
        """Получает готовые задачи из АКТИВНЫХ шардов"""
        if not self.sessions_loaded:
            return {'view_tasks': [], 'subscription_tasks': [], 'shards_processed': 0}
            
        try:
            current_time = time.time()
            view_tasks = []
            subscription_tasks = []
            shards_processed = 0
            
            # ✅ ПОЛУЧАЕМ АКТИВНЫЕ ШАРДЫ ПРОСМОТРОВ
            view_shards_processed = await self._process_view_shards(current_time)
            view_tasks.extend(view_shards_processed['tasks'])
            shards_processed += view_shards_processed['shards_count']
            
            # ✅ ПОЛУЧАЕМ АКТИВНЫЕ ШАРДЫ ПОДПИСОК
            sub_shards_processed = await self._process_subscription_shards(current_time)
            subscription_tasks.extend(sub_shards_processed['tasks'])
            shards_processed += sub_shards_processed['shards_count']
            
            if view_tasks or subscription_tasks:
                logger.info(f"""
✅ Из {shards_processed} шардов получено:
   👀 Просмотров: {len(view_tasks)}
   📺 Подписок: {len(subscription_tasks)}
                """)
            
            return {
                'view_tasks': view_tasks,
                'subscription_tasks': subscription_tasks,
                'shards_processed': shards_processed
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения задач из шардов: {e}")
            return {'view_tasks': [], 'subscription_tasks': [], 'shards_processed': 0}
    
    async def _process_view_shards(self, current_time: float) -> Dict:
        """Обрабатывает активные шарды просмотров"""
        try:
            # Получаем готовые к обработке шарды просмотров
            ready_view_shards = self.redis_client.zrangebyscore(
                "active_view_shards",
                min=0,
                max=current_time + 1800,  # +30 минут вперед
                withscores=False,
                start=0,
                num=self.max_shards_per_cycle
            )
            
            view_tasks = []
            processed_shards = 0
            
            for shard_meta_json in ready_view_shards:
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_key = shard_meta['shard_key']
                    
                    # Получаем готовые задачи из этого шарда
                    ready_task_data = self.redis_client.zrangebyscore(
                        shard_key,
                        min=0,
                        max=current_time,
                        withscores=True,
                        start=0,
                        num=self.max_tasks_per_shard
                    )
                    
                    if not ready_task_data:
                        continue
                    
                    processed_tasks = []
                    
                    for task_json, score in ready_task_data:
                        try:
                            task_data = json.loads(task_json)
                            
                            # Добавляем задачи просмотра
                            view_tasks.extend(task_data.get('tasks', []))
                            processed_tasks.append(task_json)
                            
                        except Exception as e:
                            logger.error(f"Ошибка задачи в шарде {shard_key}: {e}")
                            processed_tasks.append(task_json)
                    
                    # Удаляем обработанные задачи из шарда
                    if processed_tasks:
                        self.redis_client.zrem(shard_key, *processed_tasks)
                        processed_shards += 1
                    
                    # Если шард пустой - удаляем его из активных
                    remaining = self.redis_client.zcard(shard_key)
                    if remaining == 0:
                        self.redis_client.zrem("active_view_shards", shard_meta_json)
                        self.redis_client.delete(shard_key)
                        logger.debug(f"🗑️ Пустой шард просмотров удален: {shard_key}")
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки шарда просмотров: {e}")
            
            return {
                'tasks': view_tasks,
                'shards_count': processed_shards
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки шардов просмотров: {e}")
            return {'tasks': [], 'shards_count': 0}
    
    async def _process_subscription_shards(self, current_time: float) -> Dict:
        """Обрабатывает активные шарды подписок"""
        try:
            # Получаем готовые к обработке шарды подписок
            ready_sub_shards = self.redis_client.zrangebyscore(
                "active_subscription_shards",
                min=0,
                max=current_time + 3600,  # +1 час вперед
                withscores=False,
                start=0,
                num=self.max_shards_per_cycle
            )
            
            subscription_tasks = []
            processed_shards = 0
            
            for shard_meta_json in ready_sub_shards:
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_key = shard_meta['shard_key']
                    
                    # Получаем готовые задачи из этого шарда
                    ready_task_data = self.redis_client.zrangebyscore(
                        shard_key,
                        min=0,
                        max=current_time,
                        withscores=True,
                        start=0,
                        num=self.max_tasks_per_shard // 2  # Подписки берем меньше
                    )
                    
                    if not ready_task_data:
                        continue
                    
                    processed_tasks = []
                    
                    for task_json, score in ready_task_data:
                        try:
                            task_data = json.loads(task_json)
                            
                            # Добавляем задачи подписки
                            subscription_tasks.append(task_data)
                            processed_tasks.append(task_json)
                            
                        except Exception as e:
                            logger.error(f"Ошибка задачи в шарде {shard_key}: {e}")
                            processed_tasks.append(task_json)
                    
                    # Удаляем обработанные задачи из шарда
                    if processed_tasks:
                        self.redis_client.zrem(shard_key, *processed_tasks)
                        processed_shards += 1
                    
                    # Если шард пустой - удаляем его из активных
                    remaining = self.redis_client.zcard(shard_key)
                    if remaining == 0:
                        self.redis_client.zrem("active_subscription_shards", shard_meta_json)
                        self.redis_client.delete(shard_key)
                        logger.debug(f"🗑️ Пустой шард подписок удален: {shard_key}")
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки шарда подписок: {e}")
            
            return {
                'tasks': subscription_tasks,
                'shards_count': processed_shards
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки шардов подписок: {e}")
            return {'tasks': [], 'shards_count': 0}
    
    async def _process_view_batch_optimized(self, tasks: List[Dict]) -> int:
        """ОПТИМИЗИРОВАННАЯ обработка просмотров для шардированных задач"""
        if not tasks:
            return 0
        
        # Группируем по постам для параллельной обработки
        posts_tasks = {}
        for task in tasks:
            post_id = task.get('post_id', 'unknown')
            if post_id not in posts_tasks:
                posts_tasks[post_id] = []
            posts_tasks[post_id].append(task)
        
        logger.info(f"👀 ШАРДИРОВАННАЯ обработка {len(tasks)} просмотров с {len(posts_tasks)} постов")
        
        total_success = 0
        
        # ✅ СЕМАФОР для контроля нагрузки в зависимости от режима
        max_concurrent = {
            'conservative': 5,
            'adaptive': 10,
            'aggressive': 15
        }.get(self.performance_mode, 10)
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_post_limited(post_id, post_tasks_list):
            async with semaphore:
                return await self._process_post_views_batched(post_id, post_tasks_list)
        
        # Создаем ограниченное количество задач
        post_tasks = []
        for post_id, post_tasks_list in posts_tasks.items():
            task = asyncio.create_task(
                process_post_limited(post_id, post_tasks_list),
                name=f"post_{post_id}_sharded"
            )
            post_tasks.append(task)
        
        # Выполняем с контролем нагрузки
        results = await asyncio.gather(*post_tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, int):
                total_success += result
            elif isinstance(result, Exception):
                logger.error(f"Ошибка шардированной обработки просмотров: {result}")
        
        logger.info(f"✅ Шардированные просмотры: {total_success}/{len(tasks)} успешно")
        return total_success
    
    async def _process_post_views_batched(self, post_id: int, tasks: List[Dict]) -> int:
        """Обработка просмотров поста БАТЧАМИ для больших объемов"""
        
        # ✅ АДАПТИВНЫЙ размер батча в зависимости от режима
        batch_size = {
            'conservative': 15,
            'adaptive': 25,
            'aggressive': 35
        }.get(self.performance_mode, 25)
        
        success_count = 0
        
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            
            # Параллельно обрабатываем батч
            view_tasks = []
            for task in batch:
                delay = random.uniform(0.1, 2.0)
                view_task = asyncio.create_task(
                    self._execute_view_task_with_delay(task, delay),
                    name=f"view_{task.get('phone', 'unknown')}"
                )
                view_tasks.append(view_task)
            
            # Ждем батч
            results = await asyncio.gather(*view_tasks, return_exceptions=True)
            
            # Подсчитываем успешные в батче
            batch_success = sum(1 for r in results if isinstance(r, bool) and r)
            success_count += batch_success
            
            # Адаптивная задержка между батчами
            if i + batch_size < len(tasks):
                batch_delay = {
                    'conservative': random.uniform(2, 4),
                    'adaptive': random.uniform(1, 3),
                    'aggressive': random.uniform(0.5, 2)
                }.get(self.performance_mode, random.uniform(1, 3))
                
                await asyncio.sleep(batch_delay)
        
        logger.info(f"📊 Пост {post_id}: {success_count}/{len(tasks)} просмотров (батчами)")
        return success_count
    
    async def _process_subscription_batch_sequential(self, tasks: List[Dict]) -> int:
        """ПОСЛЕДОВАТЕЛЬНАЯ обработка подписок с ПРАВИЛЬНЫМИ паузами из файлов"""
        if not tasks:
            return 0
        
        logger.info(f"📺 ШАРДИРОВАННАЯ обработка {len(tasks)} подписок с паузами")
        
        # ✅ ЧИТАЕМ АКТУАЛЬНЫЕ НАСТРОЙКИ ИЗ ФАЙЛОВ
        accounts_delay_minutes = read_setting('accounts_delay.txt', 6.0)
        timeout_count = int(read_setting('timeout_count.txt', 4.0))
        timeout_duration_minutes = read_setting('timeout_duration.txt', 7.0)
        
        logger.info(f"""
⚙️ Настройки подписок из vars/:
   👥 Задержка между аккаунтами: {accounts_delay_minutes} мин
   🔢 Подписок до паузы: {timeout_count}
   ⏸️ Длительность паузы: {timeout_duration_minutes} мин
        """)
        
        success_count = 0
        
        for idx, task in enumerate(tasks):
            try:
                success = await self._execute_subscription_task(task)
                if success:
                    success_count += 1
                
                # ✅ ПРАВИЛЬНАЯ логика пауз
                if idx < len(tasks) - 1:  # Не ждем после последней подписки
                    
                    # Обычная задержка между аккаунтами
                    delay_seconds = accounts_delay_minutes * 60
                    
                    # ✅ ПРОВЕРЯЕМ НУЖНА ЛИ ДЛИННАЯ ПАУЗА
                    subscriptions_done = idx + 1  # Количество выполненных подписок
                    if subscriptions_done % timeout_count == 0:
                        # Каждые timeout_count подписок - длинная пауза
                        delay_seconds = timeout_duration_minutes * 60
                        logger.info(f"⏸️ ДЛИННАЯ ПАУЗА после {subscriptions_done} подписок: {timeout_duration_minutes} мин")
                    else:
                        logger.info(f"⏳ Обычная пауза: {accounts_delay_minutes} мин")
                    
                    # Коррекция задержки в зависимости от режима производительности
                    delay_multiplier = {
                        'conservative': 1.2,
                        'adaptive': 1.0,
                        'aggressive': 0.8
                    }.get(self.performance_mode, 1.0)
                    
                    final_delay = delay_seconds * delay_multiplier * random.uniform(0.9, 1.1)
                    
                    logger.info(f"⏰ Итоговая пауза: {final_delay/60:.1f} мин (режим: {self.performance_mode})")
                    await asyncio.sleep(final_delay)
                    
            except Exception as e:
                logger.error(f"Ошибка обработки подписки: {e}")
        
        logger.info(f"✅ Шардированные подписки: {success_count}/{len(tasks)} успешно")
        return success_count
    
    async def _cleanup_expired_shards(self):
        """Очищает истекшие шарды для освобождения памяти"""
        try:
            # Импортируем cleanup из task_service
            from task_service import task_service
            cleanup_stats = await task_service.cleanup_expired_shards()
            
            if cleanup_stats['expired_view_shards'] > 0 or cleanup_stats['expired_subscription_shards'] > 0:
                logger.info(f"🧹 Очищено шардов: {cleanup_stats['expired_view_shards']} просмотров, {cleanup_stats['expired_subscription_shards']} подписок")
            
        except Exception as e:
            logger.error(f"Ошибка очистки истекших шардов: {e}")
    
    async def _log_performance_stats(self, processed_views: int, processed_subs: int, processed_shards: int):
        """Логирует статистику производительности"""
        try:
            if self.sessions_loaded:
                session_stats = await global_session_manager.get_stats()
                
                # Статистика шардов
                view_shards = self.redis_client.zcard("active_view_shards") or 0
                sub_shards = self.redis_client.zcard("active_subscription_shards") or 0
                
                # Расчет производительности
                views_per_min = processed_views / 5 if processed_views > 0 else 0
                subs_per_min = processed_subs / 5 if processed_subs > 0 else 0
                shards_per_min = processed_shards / 5 if processed_shards > 0 else 0
                
                # Определение статуса производительности
                if views_per_min > 10:
                    performance_status = "🚀 ОТЛИЧНО"
                elif views_per_min > 5:
                    performance_status = "✅ ХОРОШО"
                elif views_per_min > 1:
                    performance_status = "⚠️ СРЕДНЕ"
                else:
                    performance_status = "❌ НИЗКО"
                
                logger.info(f"""
📊 ШАРДИРОВАННАЯ СТАТИСТИКА (5 мин, режим: {self.performance_mode.upper()}):
   👀 Просмотров: {processed_views} ({views_per_min:.1f}/мин)
   📺 Подписок: {processed_subs} ({subs_per_min:.1f}/мин)
   🗂️ Шардов обработано: {processed_shards} ({shards_per_min:.1f}/мин)
   🧠 Сессий: {session_stats['connected']}/{session_stats['total_loaded']}
   📋 Активных шардов: {view_shards} просмотров, {sub_shards} подписок
   🚀 Производительность: {performance_status}
                """)
            else:
                logger.info("⏳ Шардированный воркер активен, ожидаю загрузки аккаунтов...")
                
        except Exception as e:
            logger.error(f"Ошибка логирования статистики: {e}")
    
    async def _process_worker_commands(self):
        """Обрабатывает команды от бота"""
        try:
            command_data = self.redis_client.rpop('worker_commands')
            if not command_data:
                return
                
            command = json.loads(command_data)
            
            if command['command'] == 'reload_settings':
                logger.info("🔄 Получена команда обновления настроек")
                # Перенастраиваем режим производительности
                await self._detect_performance_mode()
                logger.info("✅ Настройки обновлены, режим производительности пересчитан")
                
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
                    
                    # Создаем простую тестовую задачу
                    test_task = {
                        'account_session': account['session_data'],
                        'phone': phone,
                        'channel': 'telegram',
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
    
    async def _execute_view_task_with_delay(self, task: Dict, delay: float = 0) -> bool:
        """Выполняет просмотр с задержкой"""
        if delay > 0:
            await asyncio.sleep(delay)
        
        return await self._execute_view_task(task)
    
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
            
            # Добавляем небольшую случайную задержку
            await asyncio.sleep(random.uniform(0.1, 2.0))
            
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
            await asyncio.sleep(random.uniform(1, 3))
            
            # УСПЕХ - сбрасываем счетчик неудач
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
            
            # Адаптивное количество retry задач в зависимости от режима
            max_retry = {
                'conservative': 20,
                'adaptive': 50,
                'aggressive': 80
            }.get(self.performance_mode, 50)
            
            for _ in range(max_retry):
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
    
    async def stop(self):
        """Остановка воркера"""
        logger.info("⏹️ Остановка шардированного воркера...")
        self.running = False
    
    async def _shutdown(self):
        """Корректное завершение работы"""
        logger.info("🔄 Завершение работы шардированного воркера...")
        
        try:
            # Закрываем все сессии
            await global_session_manager.shutdown()
            
            # Закрываем Redis
            if self.redis_client:
                self.redis_client.close()
            
            logger.info("✅ Шардированный воркер корректно завершен")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении: {e}")

# Создаем глобальный экземпляр воркера для совместимости
TaskWorker = ShardedTaskWorker

# Запуск воркера
async def main():
    """Главная функция шардированного воркера"""
    worker = ShardedTaskWorker()
    
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
    
    # Запускаем шардированный воркер
    asyncio.run(main())
