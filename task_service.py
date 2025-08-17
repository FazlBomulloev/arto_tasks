import asyncio
import logging
import time
import random
import math
import json
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum

from config import (
    BATCH_SIZE, read_setting, 
    find_english_word, find_lang_code
)
from database import get_accounts_by_lang, get_channels_by_lang
from session_manager import global_session_manager
from exceptions import TaskProcessingError

logger = logging.getLogger(__name__)

class TaskType(Enum):
    VIEW = "view"
    SUBSCRIBE = "subscribe"

@dataclass
class TaskItem:
    account_session: str
    phone: str
    channel: str  
    lang: str
    task_type: TaskType
    post_id: Optional[int] = None
    execute_at: Optional[float] = None
    retry_count: int = 0

class ShardedTaskService:

    def __init__(self):
        self.batch_size = BATCH_SIZE
        self.view_shard_interval = 15 * 60  
        self.subscription_shard_interval = 60 * 60 
        
    def get_view_duration(self) -> int:
        """Получает актуальную длительность просмотров из настроек"""
        hours = read_setting('followPeriod.txt', 10.0)  # Для больших нагрузок используем 10 часов
        return int(hours * 3600)
        
    async def create_view_tasks_for_post(self, channel_username: str, post_id: int) -> Dict[str, int]:
        """
        Создает задачи просмотра с ШАРДИНГОМ для масштабируемости
        
        Returns:
            Dict с результатами создания задач
        """
        results = {
            'total_tasks': 0,
            'batches_created': 0,
            'shards_created': 0,
            'languages': 0
        }
        
        try:
            # ✅ ЧИТАЕМ АКТУАЛЬНУЮ ДЛИТЕЛЬНОСТЬ ИЗ НАСТРОЕК
            view_duration = self.get_view_duration()
            view_hours = view_duration / 3600
            
            logger.info(f"📊 Настройка просмотров: {view_hours} часов (из followPeriod.txt)")
            
            # 1. Получаем языки канала из БД
            languages = await self._get_channel_languages(channel_username)
            if not languages:
                logger.error(f"⛔ Канал @{channel_username} не найден в БД")
                return results
            
            results['languages'] = len(languages)
            all_tasks = []
            
            # 2. Для каждого языка получаем аккаунты
            for lang in languages:
                english_lang = find_english_word(lang)
                accounts = await get_accounts_by_lang(english_lang, 'active')
                
                if not accounts:
                    logger.warning(f"⚠ Нет активных аккаунтов для языка {lang}")
                    continue
                
                # Создаем задачи для аккаунтов этого языка
                for account in accounts:
                    task = TaskItem(
                        account_session=account['session_data'],
                        phone=account['phone_number'],
                        channel=channel_username,
                        lang=english_lang,
                        task_type=TaskType.VIEW,
                        post_id=post_id
                    )
                    all_tasks.append(task)
            
            results['total_tasks'] = len(all_tasks)
            
            if not all_tasks:
                logger.warning("⚠ Нет задач для создания")
                return results
            
            # 3. Создаем ШАРДИРОВАННЫЕ батчи
            batches, shards_count = await self._create_sharded_view_batches(all_tasks, post_id, view_duration)
            results['batches_created'] = len(batches)
            results['shards_created'] = shards_count
            
            logger.info(f"""
📊 Создано ШАРДИРОВАННЫХ задач просмотра для поста {post_id}:
   📱 Всего задач: {results['total_tasks']}
   🌐 Языков: {results['languages']}  
   📦 Батчей: {results['batches_created']}
   🗂️ Шардов: {results['shards_created']}
   ⏰ Распределение: {view_hours} часов
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания шардированных задач просмотра: {e}")
            raise TaskProcessingError(f"Failed to create sharded view tasks: {e}")
    
    async def _get_channel_languages(self, channel_username: str) -> List[str]:
        """Получает языки канала из БД"""
        try:
            from database import db_session
            async with db_session() as conn:
                results = await conn.fetch(
                    'SELECT DISTINCT lang FROM channels WHERE name = $1',
                    channel_username
                )
                return [result['lang'] for result in results]
        except Exception as e:
            logger.error(f"Ошибка получения языков канала: {e}")
            return []
    
    async def _create_sharded_view_batches(self, all_tasks: List[TaskItem], post_id: int, duration_seconds: int) -> tuple[List[Dict], int]:
        if not all_tasks:
            return [], 0
        
        # Перемешиваем задачи для равномерности
        random.shuffle(all_tasks)
        
        total_accounts = len(all_tasks)
        
        logger.info(f"📊 Пост {post_id}: ТОЧНОЕ распределение {total_accounts} аккаунтов на {duration_seconds/3600:.1f} часов")
        
        # ✅ ТОЧНЫЙ расчет интервалов
        if total_accounts > 1:
            # Распределяем равномерно по всему периоду
            interval_per_account = duration_seconds / total_accounts
        else:
            interval_per_account = 0
        
        interval_per_account = max(interval_per_account, 30)
        
        if interval_per_account > 300:  # Больше 5 минут
            interval_per_account = 300
            logger.info(f"⚡ Ускоряю интервал до 5 минут для эффективности")
        
        logger.info(f"⏱️ Интервал между аккаунтами: {interval_per_account:.1f} секунд ({interval_per_account/60:.1f} мин)")
        
        # ✅ СОЗДАЕМ ЗАДАЧИ С ТОЧНЫМ ВРЕМЕНЕМ
        current_time = time.time()
        batches = []
        shards_map = {}
        
        for idx, task in enumerate(all_tasks):
          
            execute_at = current_time + (idx * interval_per_account)
            
            max_time = current_time + duration_seconds - 60  
            if execute_at > max_time:
                execute_at = max_time - (total_accounts - idx - 1) * 30  
            
            randomization_range = min(interval_per_account * 0.1, 30) 
            randomization = random.uniform(-randomization_range, randomization_range)
            execute_at += randomization
            
            execute_at = max(execute_at, current_time + 30) 
            execute_at = min(execute_at, current_time + duration_seconds - 30)  
            shard_interval = 900 
            shard_id = int((execute_at - current_time) // shard_interval)
            
            # Устанавливаем время выполнения
            task.execute_at = execute_at
            
            batch_info = {
                'batch_number': idx + 1,
                'tasks': [task],
                'execute_at': execute_at,
                'delay_minutes': (execute_at - current_time) / 60,
                'account_phone': task.phone,
                'post_id': post_id,
                'shard_id': shard_id
            }
            
            # Добавляем в соответствующий шард
            if shard_id not in shards_map:
                shards_map[shard_id] = []
            shards_map[shard_id].append(batch_info)
            
            batches.append(batch_info)
        
        # ✅ ПРОВЕРКА РАСПРЕДЕЛЕНИЯ
        first_time = min(batch['execute_at'] for batch in batches)
        last_time = max(batch['execute_at'] for batch in batches)
        actual_duration = (last_time - first_time) / 3600
        
        logger.info(f"""
    ✅ Пост {post_id}: создано {len(batches)} задач в {len(shards_map)} шардах
    ⏰ Первый просмотр: через {(first_time - current_time)/60:.1f} мин
    ⏰ Последний просмотр: через {(last_time - current_time)/60:.1f} мин  
    📊 Фактический период: {actual_duration:.2f} часов
    🎯 Целевой период: {duration_seconds/3600:.1f} часов
    ✅ Точность: {abs(actual_duration - duration_seconds/3600) < 0.1}
        """)
        

        await self._schedule_sharded_batches(shards_map, post_id, current_time, shard_interval)
        
        return batches, len(shards_map)
    
    async def _schedule_sharded_batches(self, shards_map: Dict[int, List[Dict]], post_id: int, 
                                      base_time: float, shard_interval: int):
        """Планирует батчи в ШАРДИРОВАННЫЕ очереди"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT, 
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            logger.info(f"📋 Планирую {len(shards_map)} шардов для поста {post_id}")
            
            total_scheduled = 0
            
            for shard_id, shard_batches in shards_map.items():
                # ✅ КЛЮЧ ШАРДА включает время начала для уникальности
                shard_start_time = base_time + (shard_id * shard_interval)
                shard_key = f"view_shard_{int(shard_start_time)}_{shard_id}"
                
                # Готовим данные для шарда
                shard_data = {}
                for batch_info in shard_batches:
                    batch_data = {
                        'batch_number': batch_info['batch_number'],
                        'execute_at': batch_info['execute_at'],
                        'post_id': post_id,
                        'task_type': 'view',
                        'shard_id': shard_id,
                        'tasks': [
                            {
                                'account_session': task.account_session,
                                'phone': task.phone,
                                'channel': task.channel,
                                'lang': task.lang,
                                'post_id': task.post_id,
                                'execute_at': task.execute_at
                            }
                            for task in batch_info['tasks']
                        ]
                    }
                    
                    shard_data[json.dumps(batch_data)] = batch_info['execute_at']
                
                # ✅ ЗАПИСЫВАЕМ ШАРД в Redis
                if shard_data:
                    redis_client.zadd(shard_key, shard_data)
                    
                    # TTL на 24 часа
                    redis_client.expire(shard_key, 24 * 3600)
                    
                    total_scheduled += len(shard_batches)
                    
                    logger.info(f"  📦 Шард {shard_id}: {len(shard_batches)} батчей → {shard_key}")
            
            # ✅ РЕГИСТРИРУЕМ АКТИВНЫЕ ШАРДЫ для воркера
            await self._register_active_shards(redis_client, shards_map.keys(), base_time, shard_interval, post_id)
            
            logger.info(f"✅ Пост {post_id}: {total_scheduled} батчей в {len(shards_map)} шардах")
            
        except Exception as e:
            logger.error(f"Ошибка планирования шардированных батчей: {e}")
            raise TaskProcessingError(f"Failed to schedule sharded batches: {e}")
    
    async def _register_active_shards(self, redis_client, shard_ids: List[int], 
                                    base_time: float, shard_interval: int, post_id: int):
        """Регистрирует активные шарды для эффективного поиска воркером"""
        try:
            active_shards_key = "active_view_shards"
            
            for shard_id in shard_ids:
                shard_start_time = base_time + (shard_id * shard_interval)
                shard_key = f"view_shard_{int(shard_start_time)}_{shard_id}"
                
                # Метаданные шарда для воркера
                shard_meta = {
                    'shard_key': shard_key,
                    'shard_id': shard_id,
                    'start_time': shard_start_time,
                    'end_time': shard_start_time + shard_interval,
                    'post_id': post_id,
                    'type': 'view'
                }
                
                # Добавляем в реестр активных шардов (сортировка по времени начала)
                redis_client.zadd(active_shards_key, {json.dumps(shard_meta): shard_start_time})
            
            # TTL на 48 часов
            redis_client.expire(active_shards_key, 48 * 3600)
            
            logger.info(f"📋 Зарегистрировано {len(shard_ids)} активных шардов")
            
        except Exception as e:
            logger.error(f"Ошибка регистрации активных шардов: {e}")
    
    async def create_subscription_tasks(self, channel_name: str, target_lang: str) -> Dict[str, int]:
        """
        Создает задачи подписки с ШАРДИНГОМ для масштабируемости
        
        Returns:
            Dict с результатами создания задач
        """
        results = {
            'total_tasks': 0,
            'accounts_processed': 0,
            'shards_created': 0
        }
        
        try:
            # Получаем аккаунты для подписки
            english_lang = find_english_word(target_lang)
            accounts = await get_accounts_by_lang(english_lang, 'active')
            
            if not accounts:
                logger.warning(f"⚠ Нет активных аккаунтов для языка {target_lang}")
                return results
            
            results['accounts_processed'] = len(accounts)
            
            # Получаем параметры задержек из настроек
            params = await self._get_subscription_delays()
            
            logger.info(f"""
📊 Параметры ШАРДИРОВАННЫХ подписок для канала @{channel_name}:
   📱 Аккаунтов: {len(accounts)}
   ⏰ Базовая задержка: {params['base_delay']/60:.1f} мин
   🎲 Разброс: ±{params['range_val']/60:.1f} мин
   👥 Между аккаунтами: {params['accounts_delay']/60:.1f} мин
   🔢 Подписок до паузы: {params['timeout_count']}
   ⏸️ Пауза: {params['timeout_duration']/60:.1f} мин
   🗂️ Шардинг: {self.subscription_shard_interval//60} мин на шард
            """)
            
            # Перемешиваем аккаунты для равномерности
            random.shuffle(accounts)
            
            # Создаем задачи с рассчитанными задержками
            subscription_tasks = []
            current_time = time.time()
            
            for account_idx, account in enumerate(accounts):
                # Рассчитываем ТОЧНУЮ задержку для этого аккаунта
                delay_seconds = await self._calculate_precise_subscription_delay(
                    account_idx, params
                )
                
                execute_at = current_time + delay_seconds
                
                task = TaskItem(
                    account_session=account['session_data'],
                    phone=account['phone_number'],
                    channel=channel_name,
                    lang=english_lang,
                    task_type=TaskType.SUBSCRIBE,
                    execute_at=execute_at
                )
                
                subscription_tasks.append(task)
            
            results['total_tasks'] = len(subscription_tasks)
            
            # ✅ ПЛАНИРУЕМ В ШАРДИРОВАННЫХ ОЧЕРЕДЯХ
            shards_count = await self._schedule_sharded_subscription_tasks(subscription_tasks, channel_name)
            results['shards_created'] = shards_count
            
            # Статистика по времени
            total_duration_hours = max(task.execute_at - current_time for task in subscription_tasks) / 3600
            
            logger.info(f"""
✅ Создано {results['total_tasks']} ШАРДИРОВАННЫХ задач подписки:
   📅 Длительность: {total_duration_hours:.1f} часов
   🗂️ Шардов: {results['shards_created']}
   ⚡ Первая подписка: через {min(task.execute_at - current_time for task in subscription_tasks)/60:.1f} мин
   🏁 Последняя подписка: через {total_duration_hours:.1f} часов
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания шардированных задач подписки: {e}")
            raise TaskProcessingError(f"Failed to create sharded subscription tasks: {e}")
    
    async def _get_subscription_delays(self) -> Dict[str, float]:
        """Получает АКТУАЛЬНЫЕ параметры задержек подписок из файлов"""
        params = {
            'base_delay': read_setting('lag.txt', 30.0) * 60,      
            'range_val': read_setting('range.txt', 5.0) * 60,      
            'accounts_delay': read_setting('accounts_delay.txt', 10.0) * 60,
            'timeout_count': int(read_setting('timeout_count.txt', 4.0)),
            'timeout_duration': read_setting('timeout_duration.txt', 20.0) * 60
        }
        
        logger.info(f"""
📋 Прочитаны настройки подписок из vars/:
   📅 Основная задержка (lag.txt): {params['base_delay']/60:.1f} мин
   🎲 Разброс (range.txt): {params['range_val']/60:.1f} мин
   👥 Между аккаунтами (accounts_delay.txt): {params['accounts_delay']/60:.1f} мин
   🔢 Подписок до паузы (timeout_count.txt): {params['timeout_count']}
   ⏸️ Длительность паузы (timeout_duration.txt): {params['timeout_duration']/60:.1f} мин
        """)
        
        return params
    
    async def _calculate_precise_subscription_delay(self, account_index: int, params: Dict[str, float]) -> float:
        """
        Рассчитывает ТОЧНУЮ задержку для подписки с ПРАВИЛЬНЫМИ паузами
        """
        base_delay = params['base_delay']           # Основная задержка (секунды)
        range_val = params['range_val']             # Разброс (секунды)
        accounts_delay = params['accounts_delay']   # Между аккаунтами (секунды)
        timeout_count = params['timeout_count']     # Подписок до паузы
        timeout_duration = params['timeout_duration'] # Длительность паузы (секунды)
        
        # 1. Базовая задержка между аккаунтами
        account_delay = account_index * accounts_delay
        
        # 2. Добавляем случайный разброс к каждому аккаунту
        random_variation = random.uniform(-range_val, range_val)
        
        # 3. ✅ ИСПРАВЛЕННЫЙ расчет пауз
        # Каждые timeout_count подписок добавляем паузу timeout_duration
        timeout_cycles = account_index // timeout_count
        timeout_delay = timeout_cycles * timeout_duration
        
        # 4. Итоговая задержка
        total_delay = account_delay + random_variation + timeout_delay
        
        # 5. Минимальная задержка - не менее базовой
        total_delay = max(total_delay, base_delay)
        
        return total_delay
    
    async def _schedule_sharded_subscription_tasks(self, tasks: List[TaskItem], channel_name: str) -> int:
        """Планирует задачи подписки в ШАРДИРОВАННЫХ очередях"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD, 
                decode_responses=True
            )
            
            # Сортируем задачи по времени выполнения
            tasks.sort(key=lambda x: x.execute_at)
            
            logger.info(f"📋 Планирую {len(tasks)} задач подписки в ШАРДИРОВАННЫХ очередях:")
            
            # ✅ ГРУППИРУЕМ ЗАДАЧИ ПО ШАРДАМ
            current_time = time.time()
            shards_map = {}
            
            for task in tasks:
                # Определяем шард по времени выполнения
                time_offset = task.execute_at - current_time
                shard_id = int(time_offset // self.subscription_shard_interval)
                
                if shard_id not in shards_map:
                    shards_map[shard_id] = []
                shards_map[shard_id].append(task)
            
            # ✅ СОЗДАЕМ ШАРДЫ И ЗАПИСЫВАЕМ В REDIS
            scheduled_count = 0
            
            for shard_id, shard_tasks in shards_map.items():
                shard_start_time = current_time + (shard_id * self.subscription_shard_interval)
                shard_key = f"sub_shard_{int(shard_start_time)}_{shard_id}"
                
                shard_data = {}
                
                for task in shard_tasks:
                    task_data = {
                        'account_session': task.account_session,
                        'phone': task.phone,
                        'channel': task.channel,
                        'lang': task.lang,
                        'task_type': 'subscription',
                        'execute_at': task.execute_at,
                        'retry_count': task.retry_count,
                        'created_at': time.time(),
                        'shard_id': shard_id
                    }
                    
                    shard_data[json.dumps(task_data)] = task.execute_at
                
                # Записываем шард в Redis
                if shard_data:
                    redis_client.zadd(shard_key, shard_data)
                    redis_client.expire(shard_key, 48 * 3600)  # TTL 48 часов
                    
                    scheduled_count += len(shard_tasks)
                    
                    avg_delay = sum(t.execute_at - current_time for t in shard_tasks) / len(shard_tasks) / 60
                    logger.info(f"  📦 Шард {shard_id}: {len(shard_tasks)} задач (среднее: {avg_delay:.1f} мин)")
            
            # ✅ РЕГИСТРИРУЕМ АКТИВНЫЕ ШАРДЫ ПОДПИСОК
            await self._register_active_subscription_shards(redis_client, shards_map.keys(), 
                                                          current_time, channel_name)
            
            logger.info(f"✅ Канал @{channel_name}: {scheduled_count} задач в {len(shards_map)} шардах")
            
            return len(shards_map)
            
        except Exception as e:
            logger.error(f"Ошибка планирования шардированных задач подписки: {e}")
            raise TaskProcessingError(f"Failed to schedule sharded subscription tasks: {e}")
    
    async def _register_active_subscription_shards(self, redis_client, shard_ids: List[int], 
                                                 base_time: float, channel_name: str):
        """Регистрирует активные шарды подписок"""
        try:
            active_shards_key = "active_subscription_shards"
            
            for shard_id in shard_ids:
                shard_start_time = base_time + (shard_id * self.subscription_shard_interval)
                shard_key = f"sub_shard_{int(shard_start_time)}_{shard_id}"
                
                shard_meta = {
                    'shard_key': shard_key,
                    'shard_id': shard_id,
                    'start_time': shard_start_time,
                    'end_time': shard_start_time + self.subscription_shard_interval,
                    'channel': channel_name,
                    'type': 'subscription'
                }
                
                redis_client.zadd(active_shards_key, {json.dumps(shard_meta): shard_start_time})
            
            redis_client.expire(active_shards_key, 48 * 3600)
            
            logger.info(f"📋 Зарегистрировано {len(shard_ids)} активных шардов подписок")
            
        except Exception as e:
            logger.error(f"Ошибка регистрации шардов подписок: {e}")
    
    async def get_task_stats(self) -> Dict[str, int]:
        """Получает статистику задач из ШАРДИРОВАННЫХ очередей"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            current_time = time.time()
            
            # Статистика по шардам просмотров
            view_shards = redis_client.zcount("active_view_shards", 0, "+inf")
            view_ready_shards = redis_client.zcount("active_view_shards", 0, current_time + 1800)  # +30 мин
            
            # Статистика по шардам подписок
            sub_shards = redis_client.zcount("active_subscription_shards", 0, "+inf")
            sub_ready_shards = redis_client.zcount("active_subscription_shards", 0, current_time + 3600)  # +1 час
            
            # Примерная оценка задач (анализируем несколько шардов)
            estimated_view_tasks = 0
            estimated_sub_tasks = 0
            
            # Берем образцы активных шардов для оценки
            sample_view_shards = redis_client.zrange("active_view_shards", 0, 5)
            for shard_meta_json in sample_view_shards:
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_size = redis_client.zcard(shard_meta['shard_key'])
                    estimated_view_tasks += shard_size
                except:
                    pass
            
            sample_sub_shards = redis_client.zrange("active_subscription_shards", 0, 5)
            for shard_meta_json in sample_sub_shards:
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_size = redis_client.zcard(shard_meta['shard_key'])
                    estimated_sub_tasks += shard_size
                except:
                    pass
            
            stats = {
                'view_shards_total': view_shards,
                'view_shards_ready': view_ready_shards,
                'subscription_shards_total': sub_shards,
                'subscription_shards_ready': sub_ready_shards,
                'estimated_view_tasks': estimated_view_tasks,
                'estimated_subscription_tasks': estimated_sub_tasks,
                'retry_queue': redis_client.llen("retry_tasks"),
                'session_pool_size': len(global_session_manager.clients),
                'session_pool_loaded': global_session_manager.loading_complete
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики шардированных задач: {e}")
            return {}
    
    async def cleanup_expired_shards(self) -> Dict[str, int]:
        """Очищает истекшие шарды для освобождения памяти"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            current_time = time.time()
            cleanup_stats = {
                'expired_view_shards': 0,
                'expired_subscription_shards': 0,
                'freed_memory_mb': 0
            }
            
            # Очищаем истекшие шарды просмотров (старше 12 часов)
            expired_view_threshold = current_time - (12 * 3600)
            expired_view_shards = redis_client.zrangebyscore(
                "active_view_shards", 
                0, 
                expired_view_threshold,
                withscores=False
            )
            
            for shard_meta_json in expired_view_shards:
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_key = shard_meta['shard_key']
                    
                    # Оцениваем размер перед удалением
                    shard_size = redis_client.zcard(shard_key)
                    cleanup_stats['freed_memory_mb'] += shard_size * 0.5 / 1024  # Примерная оценка
                    
                    # Удаляем шард
                    redis_client.delete(shard_key)
                    redis_client.zrem("active_view_shards", shard_meta_json)
                    
                    cleanup_stats['expired_view_shards'] += 1
                    
                    logger.debug(f"🗑️ Удален истекший шард просмотров: {shard_key} ({shard_size} задач)")
                    
                except Exception as e:
                    logger.error(f"Ошибка очистки шарда просмотров: {e}")
            
            # Очищаем истекшие шарды подписок (старше 48 часов)
            expired_sub_threshold = current_time - (48 * 3600)
            expired_sub_shards = redis_client.zrangebyscore(
                "active_subscription_shards", 
                0, 
                expired_sub_threshold,
                withscores=False
            )
            
            for shard_meta_json in expired_sub_shards:
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_key = shard_meta['shard_key']
                    
                    # Оцениваем размер перед удалением
                    shard_size = redis_client.zcard(shard_key)
                    cleanup_stats['freed_memory_mb'] += shard_size * 0.5 / 1024
                    
                    # Удаляем шард
                    redis_client.delete(shard_key)
                    redis_client.zrem("active_subscription_shards", shard_meta_json)
                    
                    cleanup_stats['expired_subscription_shards'] += 1
                    
                    logger.debug(f"🗑️ Удален истекший шард подписок: {shard_key} ({shard_size} задач)")
                    
                except Exception as e:
                    logger.error(f"Ошибка очистки шарда подписок: {e}")
            
            if cleanup_stats['expired_view_shards'] > 0 or cleanup_stats['expired_subscription_shards'] > 0:
                logger.info(f"""
🧹 Очистка истекших шардов:
   👀 Шардов просмотров: {cleanup_stats['expired_view_shards']}
   📺 Шардов подписок: {cleanup_stats['expired_subscription_shards']}
   💾 Освобождено памяти: {cleanup_stats['freed_memory_mb']:.1f} МБ
                """)
            
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"Ошибка очистки истекших шардов: {e}")
            return {'expired_view_shards': 0, 'expired_subscription_shards': 0, 'freed_memory_mb': 0}

# Глобальный экземпляр ШАРДИРОВАННОГО сервиса
task_service = ShardedTaskService()
